import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSClient, KVSTestFixture
from ...utils.util import Logger

BOUND = 3.0


def test_put_completes_when_backup_crashed(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        view = conductor.get_view()
        fx.broadcast_view(view)
        time.sleep(1)

        primary = fx.clients[0]

        r = primary.put("baseline", "ok")
        assert r.status_code == 200, f"baseline PUT failed: {r.status_code}"

        conductor.crash_machine(1)

        start = time.time()
        status = None
        try:
            r = requests.put(
                f"{primary.base_url}/data/after-crash",
                data="value",
                headers={"Content-Type": "text/plain"},
                allow_redirects=False,
                timeout=BOUND + 5,
            )
            status = r.status_code
        except requests.exceptions.RequestException:
            status = "hung"
        elapsed = time.time() - start

        assert status != "hung" and elapsed < BOUND, (
            f"PUT to primary took {elapsed:.2f}s with a crashed backup — "
            f"replicate_to_all retries forever instead of skipping the dead node"
        )
        assert status == 200, f"expected 200 from primary, got {status}"

        r = requests.get(f"{primary.base_url}/data/after-crash", allow_redirects=False, timeout=5)
        assert r.status_code == 200 and r.text == "value", (
            f"primary did not commit the write: status={r.status_code} body='{r.text}'"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"
