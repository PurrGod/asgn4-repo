import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSClient, KVSTestFixture
from ...utils.util import Logger

BOUND = 3.0


def test_put_completes_when_backup_partitioned(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        view = conductor.get_view()
        fx.broadcast_view(view)
        time.sleep(1)

        primary = fx.clients[0]

        r = primary.put("seed", "ok")
        assert r.status_code == 200, f"seed PUT failed: {r.status_code}"

        conductor.partition([0], "part0")
        conductor.partition([1], "part1")

        start = time.time()
        status = None
        try:
            r = requests.put(
                f"{primary.base_url}/data/while-partitioned",
                data="value",
                headers={"Content-Type": "text/plain"},
                allow_redirects=False,
                timeout=BOUND + 5,
            )
            status = r.status_code
        except requests.exceptions.RequestException:
            status = "hung"
        elapsed = time.time() - start

        assert status != "hung" and elapsed < BOUND, (
            f"PUT to the partitioned primary took {elapsed:.2f}s — "
            f"replicate_to_all never bounds its retries on an unreachable backup"
        )
        assert status == 200, (
            f"primary stays authoritative under partition and serves reads, so it must "
            f"also commit writes by skipping the unreachable backup, got {status}"
        )

        r = requests.get(f"{primary.base_url}/data/while-partitioned", allow_redirects=False, timeout=5)
        assert r.status_code == 200 and r.text == "value", (
            f"primary did not commit while partitioned: status={r.status_code} body='{r.text}'"
        )

        conductor.partition([0, 1], "base")
        time.sleep(1)
        conductor.dump_all_container_logs(dir)

    return True, "ok"
