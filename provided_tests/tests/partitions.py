import requests
import time
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture
from ..utils.util import Logger


def partitions_test(conductor: ClusterConductor, dir, log: Logger):
    """Simulate a network partition and verify nodes refuse writes and redirect reads."""
    with KVSTestFixture(conductor, dir, log, node_count=3, sync_time=5) as fx:
        fx.broadcast_view(conductor.get_view())

        c0 = fx.clients[0]
        c1 = fx.clients[1]
        c2 = fx.clients[2]

        # partition node 0 away from the rest
        conductor.partition([0], "p1")
        # allow some time for network changes to take effect
        time.sleep(1)

        # writes should be refused to maintain strong consistency
        r0 = c0.put("a", "v")
        assert r0.status_code == 503, f"expected 503 on isolated primary, got {r0.status_code}"

        r1 = c1.put("a", "v")
        assert r1.status_code == 503, f"expected 503 on partitioned follower, got {r1.status_code}"

        # followers should redirect GETs to primary (Location header points to primary)
        r = requests.get(f"{c1.base_url}/data/a", allow_redirects=False)
        assert r.status_code == 307, f"expected 307 redirect from follower, got {r.status_code}"

    return True, "ok"
