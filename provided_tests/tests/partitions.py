import requests
import time
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture, REQUEST_TIMEOUT_STATUS_CODE
from ..utils.util import Logger
from ..utils.redirect_client import RedirectAwareClient


def partitions_test(conductor: ClusterConductor, dir, log: Logger):
    """Simulate a network partition and verify nodes refuse writes and redirect reads."""
    with KVSTestFixture(conductor, dir, log, node_count=2, sync_time=5) as fx:
        fx.broadcast_view(conductor.get_view())

        c0 = RedirectAwareClient(fx.clients[0].base_url, conductor)
        c1 = RedirectAwareClient(fx.clients[1].base_url, conductor)

        # partition node 0 away from the rest
        conductor.partition([0], "p0")
        conductor.partition([1], "p1")
        # allow some time for network changes to take effect
        time.sleep(1)

        # writes should be refused to maintain strong consistency
        r = c0.put("a", "1")
        assert r.status_code == REQUEST_TIMEOUT_STATUS_CODE, (
            f"PUT should have timed out (408), got {r.status_code}"
        )

        # backups should redirect PUTs to primary (Location header points to primary)
        r = requests.put(f"{c1.base_url}/data/a", data="v", headers={"Content-Type": "text/plain"}, allow_redirects=False)
        assert r.status_code == 307, f"expected 307 redirect from follower, got {r.status_code}"

        r = c1.put("b", "1")
        assert r.status_code == REQUEST_TIMEOUT_STATUS_CODE, (
            f"PUT should have timed out (408), got {r.status_code}"
        )

        # backups should redirect GETs to primary (Location header points to primary)
        r = requests.get(f"{c1.base_url}/data/a", allow_redirects=False)
        assert r.status_code == 307, f"expected 307 redirect from follower, got {r.status_code}"

        r = requests.get(f"{c1.base_url}/data/a", allow_redirects=True)
        assert r.status_code == 404, f"expected 404 after redirect, got {r.status_code}"

        r = requests.get(f"{c0.base_url}/data/a", allow_redirects=False)
        assert r.status_code == 404, f"expected 404 from primary, got {r.status_code}"

        # heal: put all nodes back on the same network
        conductor.partition([0, 1], conductor.base_net_name)
        # allow some time for network changes to take effect
        time.sleep(5)

        r = c1.get("a")
        assert r.status_code == 200, (
            f"Expected 200 on healed backup get a, got {r.status_code}"
        )
        assert r.text == "1", f"expected value (1) but got {r.text}"

        r = c1.get("b")
        assert r.status_code == 200, (
            f"Expected 200 on healed backup get b, got {r.status_code}"
        )
        assert r.text == "1", f"expected value (1) but got {r.text}"

        r = c1.put("a", "3")
        assert r.status_code == 200, (
            f"Expected 200 on healed backup put, got {r.status_code}"
        )

        r = c1.get("a")
        assert r.status_code == 200, (
            f"Expected 200 on healed backup get 3, got {r.status_code}"
        )
        assert r.text == "3", f"expected value (3) but got {r.text}"

        conductor.dump_all_container_logs(dir)
    return True, "ok"
