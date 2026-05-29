# designed by iyyam/ajagathe/brcalcan
# Written by Claude

import asyncio

import aiohttp

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSClient, KVSTestFixture
from provided_tests.utils.util import Logger
from ..utils.redirect_client import RedirectAwareClient


async def async_put(client: RedirectAwareClient, key: str, value: str, timeout: float = 30.0):
    async with aiohttp.ClientSession() as session:
        async with session.put(
            f"{client.base_url}/data/{key}",
            data=value,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=False,
        ) as response:
            if response.status == 307:
                location = client._resolve(response.headers["Location"])
                async with session.put(
                    location,
                    data=value,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as r2:
                    return r2.status, await r2.text()
            return response.status, await response.text()


async def _run(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        fx.broadcast_view(conductor.get_view())

        clients = [RedirectAwareClient(client.base_url, conductor) for client in fx.clients]

        # --- Part 1: concurrent puts to the SAME key from all 3 nodes ---
        # Fire 30 puts (10 from each client) concurrently. The final value
        # must be one of them, and all 3 nodes must agree on it.
        log("\n> 30 CONCURRENT PUTS TO SAME KEY")
        tasks = []
        for round_i in range(10):
            for node_i, client in enumerate(clients):
                value = f"n{node_i}-r{round_i}"
                tasks.append(async_put(client, "shared", value))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful = [r for r in results if isinstance(r, tuple) and r[0] == 200]
        assert len(successful) > 0, f"no puts succeeded, got: {results}"
        log(f"  {len(successful)}/{len(results)} puts returned 200")

        # All 3 nodes must agree on the final value
        values = []
        for i, client in enumerate(clients):
            r = client.get("shared")
            assert r.status_code == 200, f"expected get 200 from node {i}, got {r.status_code}"
            values.append(r.text)
            log(f"  node {i} sees 'shared' = '{r.text}'")

        assert len(set(values)) == 1, f"nodes disagree on final value: {values}"

        # Final value must have been one of the values actually put
        valid_values = {f"n{n}-r{r}" for n in range(3) for r in range(10)}
        assert values[0] in valid_values, f"final value '{values[0]}' was never put"

        # --- Part 2: concurrent puts to DIFFERENT keys ---
        # 60 distinct keys put concurrently across all 3 clients.
        log("\n> 60 CONCURRENT PUTS TO DISTINCT KEYS")
        tasks = []
        for i in range(60):
            client = clients[i % 3]
            tasks.append(async_put(client, f"key-{i}", f"val-{i}"))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        failures = [(i, r) for i, r in enumerate(results) if not (isinstance(r, tuple) and r[0] == 200)]
        assert not failures, f"some distinct-key puts failed: {failures[:5]}"

        # Every key must be readable from every node with the right value
        for i in range(60):
            for node_i, client in enumerate(clients):
                r = client.get(f"key-{i}")
                assert r.status_code == 200, (
                    f"expected get 200 on key-{i} from node {node_i}, got {r.status_code}"
                )
                assert r.text == f"val-{i}", (
                    f"node {node_i} key-{i}: expected 'val-{i}', got '{r.text}'"
                )

        conductor.dump_all_container_logs(dir)
    return True, "ok"


def concurrent_puts(conductor: ClusterConductor, dir, log: Logger):
    return asyncio.run(_run(conductor, dir, log))
