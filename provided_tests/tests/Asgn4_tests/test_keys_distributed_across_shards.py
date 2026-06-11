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


def test_keys_distributed_across_shards(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        view = {
            "shardA": [by_id[0]],
            "shardB": [by_id[1]],
            "shardC": [by_id[2]],
        }
        for i in range(3):
            assert fx.clients[i].send_view(view).status_code == 200, f"node {i} /view failed"
        time.sleep(1)

        primaries = _shard_primary_ids(view)

        keys = [f"key-{i}" for i in range(30)]
        owner_of = {}
        for k in keys:
            owner, r = _routed_put(fx, primaries, k, f"val-{k}")
            assert owner is not None, f"no shard accepted PUT for '{k}' (got {r.status_code})"
            owner_of[k] = owner

        shards_used = set(owner_of.values())
        assert len(shards_used) >= 2, (
            f"expected keys spread over >=2 shards, all landed on {shards_used} — "
            f"sharding/routing likely not active"
        )

        for k in keys:
            owner, r = _routed_get(fx, primaries, k)
            assert r.status_code == 200, f"GET '{k}' got {r.status_code}, expected 200"
            assert r.text == f"val-{k}", f"GET '{k}' got '{r.text}'"
            assert owner == owner_of[k], (
                f"'{k}' written to shard {owner_of[k]} but read from {owner}"
            )

        conductor.dump_all_container_logs(dir)

    return True, "ok"
