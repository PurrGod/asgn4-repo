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


def test_multishard_view_install(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        view = {
            "shardA": [by_id[0]],
            "shardB": [by_id[1]],
        }
        for i in range(2):
            r = fx.clients[i].send_view(view)
            assert r.status_code == 200, f"node {i}: PUT /view got {r.status_code}"
            assert r.text == "", f"node {i}: expected empty body, got '{r.text}'"
        time.sleep(1)

        primaries = _shard_primary_ids(view)

        owner, r = _routed_put(fx, primaries, "hello", "world")
        assert owner is not None, (
            f"no shard accepted the PUT directly (all redirected): {r.status_code}"
        )

        owner_get, r = _routed_get(fx, primaries, "hello")
        assert r.status_code == 200, f"expected 200 from owning shard, got {r.status_code}"
        assert r.text == "world", f"expected 'world', got '{r.text}'"
        assert owner_get == owner, (
            f"GET resolved to shard primary {owner_get} but PUT landed on {owner}"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"
