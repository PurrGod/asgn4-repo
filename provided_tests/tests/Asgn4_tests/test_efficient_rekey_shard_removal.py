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


def test_efficient_rekey_on_shard_removal(conductor: ClusterConductor, dir, log: Logger):
    # Going from 5 shards down to 4 means only the keys whose ring arc was owned by the 
    # removed shard should have to move.
    with KVSTestFixture(conductor, dir, log, node_count=5) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        before_view = {
            "s0": [by_id[0]],
            "s1": [by_id[1]],
            "s2": [by_id[2]],
            "s3": [by_id[3]],
            "s4": [by_id[4]],
        }
        for i in range(5):
            rv = fx.clients[i].send_view(before_view)
            assert rv is not None, f"send_view returned None for node {i} (before view)"
            assert rv.status_code == 200, f"node {i} /view failed: {rv.status_code}"
        time.sleep(1)

        primaries_before = _shard_primary_ids(before_view)

        n_keys = 100
        keys = [f"shrink-{i}" for i in range(n_keys)]
        for k in keys:
            o, _ = _routed_put(fx, primaries_before, k, "x")
            assert o is not None, f"PUT '{k}' not accepted by any shard"

        # Snapshot of where each key lives before the change is made
        before_loc = {}
        for node_id in range(5):
            for k in _node_keys(fx, node_id):
                before_loc[k] = node_id
        stored = len(before_loc)
        assert stored == n_keys, (
            f"expected {n_keys} keys stored across shards before change, found {stored}"
        )

        # Node 4 is no longer in any shard but still receives the view so it can release its keys.
        after_view = {
            "s0": [by_id[0]],
            "s1": [by_id[1]],
            "s2": [by_id[2]],
            "s3": [by_id[3]],
        }
        for i in range(5):
            rv = fx.clients[i].send_view(after_view)
            assert rv is not None, f"send_view returned None for node {i} (after view)"
            assert rv.status_code == 200, f"node {i} /view (after) failed: {rv.status_code}"
        time.sleep(2)

        after_by_node = {nid: _node_keys(fx, nid) for nid in range(5)}

        # A key moved if it's no longer on the node that previously held it.
        # Every key that lived on s4 (node 4) must have moved and
        # the consistent-hashing guarantee is that almost nothing else moved.
        moved = 0
        for k, old_node in before_loc.items():
            if k not in after_by_node[old_node]:
                moved += 1
        frac = moved / n_keys

        assert frac < 0.5, (
            f"{moved}/{n_keys} ({frac:.0%}) keys moved going 5->4 shards; "
            f"a consistent-hashing scheme should move ~20% (just the removed "
            f"shard's slice), a random reshuffle would move ~80%"
        )

        # Every surviving shard should still hold some keys (no shard ended
        # up empty just because we removed s4)
        for nid in range(4):
            assert len(after_by_node[nid]) > 0, (
                f"surviving shard on node {nid} ended up empty after removal — "
                f"key redistribution skewed all traffic onto a subset of shards"
            )

        # The removed node should no longer be authoritative for any
        # owned keys
        # We don't assert node 4's /internal/keys is empty because the server
        # keeps an all_known_store cache; instead we verify
        # the remaining shards together still cover every key.
        survivors_union = set()
        for nid in range(4):
            survivors_union |= after_by_node[nid]
        missing = set(keys) - survivors_union
        assert not missing, (
            f"{len(missing)} keys are not held by any surviving shard after "
            f"removal: {sorted(missing)[:5]}..."
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"