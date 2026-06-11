import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSClient, KVSTestFixture
from ...utils.util import Logger


def test_replication_is_synchronous(conductor: ClusterConductor, dir, log: Logger):
    # Strong consistency says that by the time a PUT to the primary
    # returns 200, every available backup in the shard has the new value.
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        view = {
            "shardA": [by_id[0], by_id[1]],
        }
        for i in range(2):
            assert fx.clients[i].send_view(view).status_code == 200, (
                f"node {i} /view failed"
            )
        time.sleep(1)

        primary_id = min(n["id"] for n in view["shardA"])
        backup_id = max(n["id"] for n in view["shardA"])

        # PUT to the primary.
        r = requests.put(
            f"{fx.clients[primary_id].base_url}/data/sync-key",
            data="sync-value",
            headers={"Content-Type": "text/plain"},
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 200, f"PUT to primary failed: {r.status_code}"

        # Immediately ask the backup.
        # We probe the backup's store via /internal/keys, which reads from
        # store (merged with all_known_store) where the key must be.
        r = requests.get(
            f"{fx.clients[backup_id].base_url}/internal/keys",
            timeout=5,
        )
        assert r.status_code == 200, (
            f"backup /internal/keys returned {r.status_code}"
        )
        import json
        backup_store = json.loads(r.text)
        assert "sync-key" in backup_store, (
            f"backup did not receive 'sync-key' synchronously: "
            f"primary returned 200 but backup's store has {list(backup_store.keys())}"
        )
        assert backup_store["sync-key"] == "sync-value", (
            f"backup has wrong value: expected 'sync-value', "
            f"got '{backup_store['sync-key']}'"
        )

        # Update the same key. Backup should reflect the latest value.
        r = requests.put(
            f"{fx.clients[primary_id].base_url}/data/sync-key",
            data="updated-value",
            headers={"Content-Type": "text/plain"},
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 200, f"PUT update failed: {r.status_code}"

        r = requests.get(
            f"{fx.clients[backup_id].base_url}/internal/keys",
            timeout=5,
        )
        backup_store = json.loads(r.text)
        assert backup_store.get("sync-key") == "updated-value", (
            f"backup did not synchronously receive the updated value: "
            f"got '{backup_store.get('sync-key')}'"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"