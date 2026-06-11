import json
import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSClient, KVSTestFixture
from ...utils.util import Logger


def test_new_node_onboarding(conductor: ClusterConductor, dir, log: Logger):
    # When a fresh node joins an existing shard via a view change,
    # it must receive the existing keys for that shard from its peers
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        # Start: shardA = node 0, shardB = node 1. Node 2 is up but in no shard
        before_view = {
            "shardA": [by_id[0]],
            "shardB": [by_id[1]],
        }
        for i in range(3):
            assert fx.clients[i].send_view(before_view).status_code == 200, (
                f"node {i} /view (before) failed"
            )
        time.sleep(1)

        # Populate keys across both shards
        primaries_before = [min(n["id"] for n in nodes) for nodes in before_view.values()]
        n_keys = 40
        keys = {}
        for i in range(n_keys):
            k = f"onboard-{i}"
            v = f"value-{i}"
            # Try each primary until one accepts
            for pid in primaries_before:
                r = requests.put(
                    f"{fx.clients[pid].base_url}/data/{k}",
                    data=v,
                    headers={"Content-Type": "text/plain"},
                    allow_redirects=False,
                    timeout=5,
                )
                if r.status_code == 200:
                    keys[k] = v
                    break
        assert len(keys) == n_keys, f"only {len(keys)}/{n_keys} keys accepted"

        # Add node 2 to shardA. Node 2 must learn shardA's keys
        after_view = {
            "shardA": [by_id[0], by_id[2]],
            "shardB": [by_id[1]],
        }
        for i in range(3):
            assert fx.clients[i].send_view(after_view).status_code == 200, (
                f"node {i} /view (after) failed"
            )
        time.sleep(2)

        # Find a shardA key and confirm node 2 has it in its store
        r = requests.get(f"{fx.clients[2].base_url}/internal/keys", timeout=5)
        assert r.status_code == 200
        node2_store = json.loads(r.text)

        # Pick keys that node 0 (shardA's primary) serves
        # directly with 200 which are the shardA keys node 2 should now have
        shardA_keys = []
        for k in keys:
            r = requests.get(
                f"{fx.clients[0].base_url}/data/{k}",
                allow_redirects=False,
                timeout=5,
            )
            if r.status_code == 200:
                shardA_keys.append(k)

        assert shardA_keys, "no shardA keys found — ring distribution skewed"

        missing = [k for k in shardA_keys if k not in node2_store]
        assert not missing, (
            f"new node 2 joined shardA but is missing {len(missing)}/{len(shardA_keys)} "
            f"shardA keys: {missing[:5]} — view-change state sync didn't pull keys "
            f"from existing shardA members"
        )

        # Checks that the values are correct, not just present
        for k in shardA_keys[:10]:
            assert node2_store[k] == keys[k], (
                f"new node 2 has wrong value for '{k}': "
                f"expected '{keys[k]}', got '{node2_store[k]}'"
            )

        # After onboarding, writes should replicate to node 2 too
        r = requests.put(
            f"{fx.clients[0].base_url}/data/post-onboard",
            data="fresh",
            headers={"Content-Type": "text/plain"},
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 200, f"post-onboard PUT failed: {r.status_code}"

        r = requests.get(f"{fx.clients[2].base_url}/internal/keys", timeout=5)
        node2_store = json.loads(r.text)
        assert node2_store.get("post-onboard") == "fresh", (
            f"new node didn't receive replication of post-onboard write: "
            f"got '{node2_store.get('post-onboard')}'"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"