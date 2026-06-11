import json
import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSClient, KVSTestFixture
from ...utils.util import Logger

# to identify the primary node id for each shard
def _shard_primary_ids(view):
    result = {}
    for sid, nodes in view.items():
        smallest = min(n["id"] for n in nodes)
        result[sid] = smallest
    return result
  

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

def test_put_replicates_only_inside_owning_shard(
    conductor: ClusterConductor, dir, log: Logger
):
    with KVSTestFixture(conductor, dir, log, node_count=4) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        view = {
            "shardA": [by_id[0], by_id[1]],
            "shardB": [by_id[2], by_id[3]],
        }

        for i in range(4):
            assert fx.clients[i].send_view(view).status_code == 200

        time.sleep(1)

        sid_primary = _shard_primary_ids(view)
        primaries = list(sid_primary.values())

        chosen_key = None
        owner = None

        for i in range(100):
            key = f"replication-key-{i}"
            owner, r = _routed_put(fx, primaries, key, "replicated-value")
            assert r.status_code == 200

            if owner == sid_primary["shardA"]:
                chosen_key = key
                break

        assert chosen_key is not None, "could not find key owned by shardA"

        for node in view["shardA"]:
            r = requests.get(
                f"{fx.clients[node['id']].base_url}/data/{chosen_key}",
                allow_redirects=False,
                timeout=5,
            )
            assert r.status_code == 200
            assert r.text == "replicated-value"

        for node in view["shardB"]:
            r = requests.get(
                f"{fx.clients[node['id']].base_url}/data/{chosen_key}",
                allow_redirects=False,
                timeout=5,
            )
            assert r.status_code == 307

        conductor.dump_all_container_logs(dir)

    return True, "ok"