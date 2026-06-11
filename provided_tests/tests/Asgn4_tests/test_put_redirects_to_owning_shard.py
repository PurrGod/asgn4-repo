import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSClient, KVSTestFixture
from ...utils.util import Logger


def _shard_primary_ids(view):
    return {sid: min(n["id"] for n in nodes) for sid, nodes in view.items()}


def _routed_put(fx, primary_ids, key, value):
    last = None
    for pid in primary_ids:
        r = requests.put(
            f"{fx.clients[pid].base_url}/data/{key}",
            data=value,
            headers={"Content-Type": "text/plain"},
            allow_redirects=False,
            timeout=5,
        )
        last = r
        if r.status_code == 200:
            return pid, r
    return None, last


def test_put_redirects_to_owning_shard(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        view = {
            "shardA": [by_id[0]],
            "shardB": [by_id[1]],
        }
        for i in range(2):
            assert fx.clients[i].send_view(view).status_code == 200, f"node {i} /view failed"
        time.sleep(1)

        sid_primary = _shard_primary_ids(view)
        primaries = list(sid_primary.values())

        owner = None
        chosen_key = None
        for i in range(40):
            k = f"pprobe-{i}"
            o, _ = _routed_put(fx, primaries, k, "seed")
            assert o is not None, f"PUT '{k}' not accepted by any shard"
            if o == sid_primary["shardB"]:
                owner, chosen_key = o, k
                break
        assert chosen_key is not None, "could not find a key owned by shardB"

        wrong_node = sid_primary["shardA"]
        r = requests.put(
            f"{fx.clients[wrong_node].base_url}/data/{chosen_key}",
            data="rerouted",
            headers={"Content-Type": "text/plain"},
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 307, f"PUT to wrong shard should 307, got {r.status_code}"
        owner_addr = by_id[owner]["address"]
        expected = f"http://{owner_addr}/data/{chosen_key}"
        assert r.headers.get("Location") == expected, (
            f"expected redirect to {expected}, got {r.headers.get('Location')}"
        )

        r = requests.put(
            f"{fx.clients[owner].base_url}/data/{chosen_key}",
            data="rerouted",
            headers={"Content-Type": "text/plain"},
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 200, (
            f"owning shard primary must handle the PUT directly, got {r.status_code}"
        )

        r = requests.get(
            f"{fx.clients[owner].base_url}/data/{chosen_key}",
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 200 and r.text == "rerouted", (
            f"expected 'rerouted', got status={r.status_code} body='{r.text}'"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"
