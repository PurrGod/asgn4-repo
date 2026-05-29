import requests
import time
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture
from ..utils.util import Logger


def simple_partition(conductor: ClusterConductor, dir, log: Logger):
    """Test /view handling: malformed JSON and proper view acceptance."""
    with KVSTestFixture(conductor, dir, log, node_count=2, sync_time=5) as fx:
        fx.broadcast_view(conductor.get_view())

        c0 = fx.clients[0]
        c1 = fx.clients[1]

        r = c0.put("a", "1")
        assert r.status_code == 200, f"expected 200 on primary, got {r.status_code}"

        r = c0.get("a")
        assert r.status_code == 200, f"expected 200 on primary, got {r.status_code}"
        assert r.text == "1", f"expected value '1', got {r.text}"

        r = c1.get("a")
        assert r.status_code == 200, f"expected 200 on primary, got {r.status_code}"
        assert r.text == "1", f"expected value '1', got {r.text}"

        # partition each node into its own network
        conductor.partition([0], "p0")
        conductor.partition([1], "p1")

        conductor.describe_cluster()

        # allow some time for network changes to take effect
        time.sleep(1)

        # writes should be refused to maintain strong consistency
        r = c0.put("a", "2")
        assert r.status_code == 503, f"expected 503 on isolated primary, got {r.status_code}"

        r = c1.put("a", "2")
        assert r.status_code == 503, f"expected 503 on partitioned follower, got {r.status_code}"

        r = c0.get("a")
        assert r.status_code == 200, f"expected 200 on isolated primary get, got {r.status_code}"
        assert r.text == "1", f"expected value '1', got {r.text}"

        # backups should redirect GETs to primary (Location header points to primary)
        r = requests.get(f"{c1.base_url}/data/a", allow_redirects=False)
        assert r.status_code == 307, f"expected 307 redirect from backup, got {r.status_code}"

        # This should return 200 since the redirect should still work
        r = c1.get("a")
        assert r.status_code == 200, f"expected 200 on isolated backup get, got {r.status_code}"
        assert r.text == "1", f"expected value '1', got {r.text}"

        # heal: put all nodes back on the same network
        conductor.partition([0, 1], conductor.base_net_name)

        # allow some time for network changes to take effect
        time.sleep(1)

        r = c0.put("a", "2")
        assert r.status_code == 200, f"expected 200 on primary, got {r.status_code}"

        r = c1.put("a", "3")
        assert r.status_code == 200, f"expected 200 on backup, got {r.status_code}"

        r = c0.get("a")
        assert r.status_code == 200, f"expected 200 on healed primary get, got {r.status_code}"
        assert r.text == "3", f"expected value '3', got {r.text}"

        r = c1.get("a")
        assert r.status_code == 200, f"expected 200 on healed backup get, got {r.status_code}"
        assert r.text == "3", f"expected value '3', got {r.text}"

        conductor.describe_cluster()

        # conductor.get_partition_view()
        # fx.send_view(0, conductor.get_partition_view("p0"))
        # fx.send_view(0, conductor.get_partition_view("p1"))
        # fx.broadcast_view(conductor.get_view())
        # time.sleep(1)

    return True, "ok"
