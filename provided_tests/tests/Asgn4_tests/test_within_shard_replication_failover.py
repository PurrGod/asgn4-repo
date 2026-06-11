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


def test_within_shard_replication_failover(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}


        view = {
            "shardA": [by_id[0], by_id[1]],
            "shardB": [by_id[2]],
        }
        for i in range(3):
            assert fx.clients[i].send_view(view).status_code == 200, f"node {i} /view failed"
        time.sleep(1)

        sid_primary = _shard_primary_ids(view)
        primaries = list(sid_primary.values())

        chosen = None
        for i in range(40):
            k = f"fo-{i}"
            o, _ = _routed_put(fx, primaries, k, f"value-{i}")
            assert o is not None, f"PUT '{k}' not accepted"
            if o == sid_primary["shardA"]:
                chosen = (k, f"value-{i}")
                break
        assert chosen is not None, "could not find a key owned by shardA"
        key, val = chosen


        conductor.crash_machine(0)

        new_view = {
            "shardA": [by_id[1]],
            "shardB": [by_id[2]],
        }
        fx.send_view(1, new_view)
        fx.send_view(2, new_view)
        time.sleep(1)


        r = requests.get(
            f"{fx.clients[1].base_url}/data/{key}",
            allow_redirects=False, timeout=5,
        )
        assert r.status_code == 200, (
            f"promoted shardA node expected 200 for '{key}', got {r.status_code} — "
            f"within-shard replication may not have happened"
        )
        assert r.text == val, f"expected '{val}', got '{r.text}'"

        r = requests.put(
            f"{fx.clients[1].base_url}/data/{key}",
            data="post-failover", headers={"Content-Type": "text/plain"},
            allow_redirects=False, timeout=5,
        )
        assert r.status_code == 200, f"post-failover PUT failed: {r.status_code}"

        conductor.dump_all_container_logs(dir)

    return True, "ok"
