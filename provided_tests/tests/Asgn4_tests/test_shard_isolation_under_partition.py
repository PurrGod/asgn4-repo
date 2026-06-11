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


def test_shard_isolation_under_partition(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=4) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        view = {
            "shardA": [by_id[0], by_id[1]],
            "shardB": [by_id[2], by_id[3]],
        }
        for i in range(4):
            assert fx.clients[i].send_view(view).status_code == 200, f"node {i} /view failed"
        time.sleep(1)

        sid_primary = _shard_primary_ids(view)
        primaries = list(sid_primary.values())

        b_keys, a_keys = [], []
        for i in range(40):
            k = f"iso-{i}"
            o, _ = _routed_put(fx, primaries, k, f"orig-{i}")
            assert o is not None, f"PUT '{k}' not accepted"
            if o == sid_primary["shardB"]:
                b_keys.append((k, f"orig-{i}"))
            elif o == sid_primary["shardA"]:
                a_keys.append((k, f"orig-{i}"))
        assert b_keys, "no keys mapped to shardB"
        assert a_keys, "no keys mapped to shardA"

        a_backup = max(n["id"] for n in view["shardA"])
        conductor.crash_machine(a_backup)

        b_primary = sid_primary["shardB"]
        bk, bv = b_keys[0]

        start = time.time()
        r = requests.get(
            f"{fx.clients[b_primary].base_url}/data/{bk}",
            allow_redirects=False, timeout=3,
        )
        elapsed = time.time() - start
        assert r.status_code == 200 and r.text == bv, (
            f"shardB read broke after a shardA node crashed: "
            f"status={r.status_code} body='{r.text}'"
        )
        assert elapsed < 3.0, f"shardB read took {elapsed:.2f}s after shardA crash"

        start = time.time()
        r = requests.put(
            f"{fx.clients[b_primary].base_url}/data/{bk}",
            data="updated", headers={"Content-Type": "text/plain"},
            allow_redirects=False, timeout=3,
        )
        elapsed = time.time() - start
        assert r.status_code == 200, (
            f"shardB write should succeed after a shardA node crashed, got {r.status_code}"
        )
        assert elapsed < 3.0, f"shardB write took {elapsed:.2f}s after shardA crash"

        r = requests.get(
            f"{fx.clients[b_primary].base_url}/data/{bk}",
            allow_redirects=False, timeout=3,
        )
        assert r.status_code == 200 and r.text == "updated", (
            f"shardB did not reflect the update: status={r.status_code} body='{r.text}'"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"