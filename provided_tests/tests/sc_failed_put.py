import requests
import time
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture, REQUEST_TIMEOUT_STATUS_CODE
from ..utils.util import Logger


def sc_failed_put(conductor: ClusterConductor, dir, log: Logger):
    """Test /view handling: malformed JSON and proper view acceptance."""
    with KVSTestFixture(conductor, dir, log, node_count=3, sync_time=5) as fx:
        fx.broadcast_view(conductor.get_view())

        c0 = fx.clients[0]
        c1 = fx.clients[1]
        c2 = fx.clients[2]

        r = c0.put("a", "1")
        assert r.status_code == 200, f"expected 200 on primary, got {r.status_code}"

        # partition node 2 away from 0 and 1
        conductor.partition([0, 1], "p01")
        conductor.partition([2], "p2")
        conductor.describe_cluster()

        # allow some time for network changes to take effect
        time.sleep(1)

        r = c1.put("a", "2")
        assert r.status_code == REQUEST_TIMEOUT_STATUS_CODE, (
            f"PUT should have timed out (408), got {r.status_code}"
        )

        r = c0.get("a")
        assert r.status_code == 200, f"expected 200 on only node in updated view get, got {r.status_code}"
        assert r.text == "1", f"expected value '1', got {r.text}"

        r = c1.get("a")
        assert r.status_code == 200, f"expected 200 on only node in updated view get, got {r.status_code}"
        assert r.text == "1", f"expected value '1', got {r.text}"

        r = c2.get("a")
        assert r.status_code == 200, f"expected 200 on only node in updated view get, got {r.status_code}"
        assert r.text == "1", f"expected value '1', got {r.text}"

    return True, "ok"
