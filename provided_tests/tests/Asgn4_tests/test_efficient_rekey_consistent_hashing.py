import json
import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSClient, KVSTestFixture
from ...utils.util import Logger


def _shard_primary_ids(view):
    return [min(n["id"] for n in nodes) for nodes in view.values()]


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


def _node_keys(fx, node_id):

    base = fx.clients[node_id].base_url
    r = requests.get(f"{base}/internal/keys", timeout=5)
    if r.status_code != 200:
        return set()
    return set(json.loads(r.text).keys())


def test_efficient_rekey_consistent_hashing(conductor: ClusterConductor, dir, log: Logger):

    with KVSTestFixture(conductor, dir, log, node_count=5) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        before_view = {
            "s0": [by_id[0]],
            "s1": [by_id[1]],
            "s2": [by_id[2]],
            "s3": [by_id[3]],
        }
        for i in range(4):
            rv = fx.clients[i].send_view(before_view)
            assert rv is not None, f"send_view returned None for node {i} (before view)"
            assert rv.status_code == 200, f"node {i} /view failed: {rv.status_code}"
        time.sleep(1)

        primaries_before = _shard_primary_ids(before_view)

        n_keys = 100
        keys = [f"rk-{i}" for i in range(n_keys)]
        for k in keys:
            o, _ = _routed_put(fx, primaries_before, k, "x")
            assert o is not None, f"PUT '{k}' not accepted by any shard"


        before_loc = {}
        for node_id in range(4):
            for k in _node_keys(fx, node_id):
                before_loc[k] = node_id
        stored = len(before_loc)
        assert stored == n_keys, (
            f"expected {n_keys} keys stored across shards before change, found {stored}"
        )


        after_view = dict(before_view)
        after_view["s4"] = [by_id[4]]
        for i in range(5):
            rv = fx.clients[i].send_view(after_view)
            assert rv is not None, f"send_view returned None for node {i} (after view)"
            assert rv.status_code == 200, f"node {i} /view (after) failed: {rv.status_code}"
        time.sleep(2)

        after_by_node = {nid: _node_keys(fx, nid) for nid in range(5)}

        moved = 0
        for k, old_node in before_loc.items():
            if k not in after_by_node[old_node]:
                moved += 1
        frac = moved / n_keys

        assert frac < 0.5, (
            f"{moved}/{n_keys} ({frac:.0%}) keys moved going 4->5 shards; "
            f"a consistent-hashing scheme should move ~20%, a random reshuffle ~80%"
        )
        assert len(after_by_node[4]) > 0, (
            "the new shard (node 4) received no keys — sharding/rehash not taking effect"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"