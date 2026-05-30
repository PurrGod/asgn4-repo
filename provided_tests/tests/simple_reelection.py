import requests
import time
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture, REQUEST_TIMEOUT_STATUS_CODE
from ..utils.util import Logger


def simple_reelection(conductor: ClusterConductor, dir, log: Logger):
    """Test /view handling: malformed JSON and proper view acceptance."""
    with KVSTestFixture(conductor, dir, log, node_count=2, sync_time=5) as fx:
        fx.broadcast_view(conductor.get_view())

        c0 = fx.clients[0]
        c1 = fx.clients[1]

        r = c0.put("a", "1")
        assert r.status_code == 200, f"expected 200 on primary, got {r.status_code}"

        # partition each node into its own network
        conductor.partition([0], "p0")
        conductor.partition([1], "p1")
        conductor.describe_cluster()

        # allow some time for network changes to take effect
        time.sleep(1)

        # writes should be refused to maintain strong consistency
        r = c0.put("a", "2")
        assert r.status_code == REQUEST_TIMEOUT_STATUS_CODE, (
            f"PUT should have timed out (408), got {r.status_code}"
        )

        r = c1.put("a", "2")
        assert r.status_code == REQUEST_TIMEOUT_STATUS_CODE, (
            f"PUT should have timed out (408), got {r.status_code}"
        )

        # send view so that only node 1 is the only node in view
        old_view = conductor.get_view()
        new_view = {"defaultShard": [old_view["defaultShard"][1]]}
        fx.broadcast_view(new_view)
        time.sleep(1)

        # should be allowed to put in node 1 because it is its own primary
        r = c1.put("a", "2")
        assert r.status_code == 200, f"expected 200 on only node in updated view put, got {r.status_code}"

        r = c1.get("a")
        assert r.status_code == 200, f"expected 200 on only node in updated view get, got {r.status_code}"
        assert r.text == "2", f"expected value '2', got {r.text}"

    return True, "ok"
