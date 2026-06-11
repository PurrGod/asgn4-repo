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


def _routed_get(fx, primary_ids, key):
    last = None
    for pid in primary_ids:
        r = requests.get(
            f"{fx.clients[pid].base_url}/data/{key}",
            allow_redirects=False,
            timeout=5,
        )
        last = r
        if r.status_code in (200, 404):
            return pid, r
    return None, last


def test_node_moves_between_shards(conductor: ClusterConductor, dir, log: Logger):
    # Spec says that machines can change shards. A node leaving one
    # shard for another must stop owning its old keys and start owning whatever
    # the new shard membership makes it responsible for.
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        # Start: node 2 is a backup in shardA alongside node 0; shardB is node 1.
        before_view = {
            "shardA": [by_id[0], by_id[2]],
            "shardB": [by_id[1]],
        }
        for i in range(3):
            assert fx.clients[i].send_view(before_view).status_code == 200, (
                f"node {i} /view (before) failed"
            )
        time.sleep(1)

        primaries_before = _shard_primary_ids(before_view)

        n_keys = 60
        keys = {f"mv-{i}": f"value-{i}" for i in range(n_keys)}
        for k, v in keys.items():
            owner, _ = _routed_put(fx, primaries_before, k, v)
            assert owner is not None, f"PUT '{k}' not accepted"

        # Move node 2 out of shardA and into shardB.
        after_view = {
            "shardA": [by_id[0]],
            "shardB": [by_id[1], by_id[2]],
        }
        for i in range(3):
            assert fx.clients[i].send_view(after_view).status_code == 200, (
                f"node {i} /view (after) failed"
            )
        time.sleep(2)

        primaries_after = _shard_primary_ids(after_view)

        # All keys still reachable and correct from whichever shard now owns them.
        for k, v in keys.items():
            owner, r = _routed_get(fx, primaries_after, k)
            assert r is not None and r.status_code == 200, (
                f"key '{k}' unreachable after node move: "
                f"status={None if r is None else r.status_code}"
            )
            assert r.text == v, f"key '{k}' wrong: expected '{v}', got '{r.text}'"

        # Node 2 is now in shardB. For any key that hashes to
        # shardA (i.e. node 2 used to own it as a backup), hitting node 2's
        # /data/ endpoint directly must NOT serve the value — node 2 should
        # 307 redirect to shardA's primary, proving it correctly disowned the
        # key on the view change. If node 2 still serves 200, it kept stale
        # shardA data instead of re-sorting its store.
        shardA_primary_addr = by_id[0]["address"]
        checked = 0
        for k in keys:
            # Ask shardA's primary directly whether it owns this key (it will
            # serve 200 if yes, or 307 to elsewhere if not — but since shardA
            # primary IS node 0, a 200 means k belongs to shardA)
            r = requests.get(
                f"{fx.clients[0].base_url}/data/{k}",
                allow_redirects=False,
                timeout=5,
            )
            if r.status_code != 200:
                continue  # not a shardA key, skip
            # k is a shardA key so check node 2 to see if it disowned it
            r2 = requests.get(
                f"{fx.clients[2].base_url}/data/{k}",
                allow_redirects=False,
                timeout=5,
            )
            assert r2.status_code != 200, (
                f"node 2 still serves shardA key '{k}' after moving to shardB — "
                f"didn't drop it from store on the view change"
            )
            if r2.status_code == 307:
                expected = f"http://{shardA_primary_addr}/data/{k}"
                assert r2.headers.get("Location") == expected, (
                    f"node 2 redirected '{k}' to {r2.headers.get('Location')}, "
                    f"expected {expected}"
                )
            checked += 1
            if checked >= 10:
                break

        assert checked > 0, (
            "no shardA keys found to test disownership against — "
            "ring distribution skewed everything onto one shard"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"