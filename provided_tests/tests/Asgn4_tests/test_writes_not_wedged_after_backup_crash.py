import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSClient, KVSTestFixture
from ...utils.util import Logger

BOUND = 3.0


def _put(url, value):
    start = time.time()
    try:
        r = requests.put(
            url, data=value, headers={"Content-Type": "text/plain"},
            allow_redirects=False, timeout=BOUND + 5,
        )
        return r.status_code, time.time() - start
    except requests.exceptions.RequestException:
        return "hung", time.time() - start


def test_writes_not_wedged_after_backup_crash(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        view = conductor.get_view()
        fx.broadcast_view(view)
        time.sleep(1)

        primary = fx.clients[0]
        conductor.crash_machine(1)

        status, elapsed = _put(f"{primary.base_url}/data/key-a", "a")
        assert status != "hung" and elapsed < BOUND, (
            f"first PUT after crash took {elapsed:.2f}s (status={status})"
        )

        status, elapsed = _put(f"{primary.base_url}/data/key-b", "b")
        assert status != "hung" and elapsed < BOUND, (
            f"second PUT to a different key took {elapsed:.2f}s — "
            f"write_lock is held by a forever-retrying replicate, wedging all writes"
        )
        assert status == 200, f"expected 200, got {status}"

        status, elapsed = _put(f"{primary.base_url}/data/key-c", "c")
        assert status == 200 and elapsed < BOUND, (
            f"third PUT failed/hung: status={status} elapsed={elapsed:.2f}s"
        )

        for k, v in (("key-a", "a"), ("key-b", "b"), ("key-c", "c")):
            r = requests.get(f"{primary.base_url}/data/{k}", allow_redirects=False, timeout=5)
            assert r.status_code == 200 and r.text == v, (
                f"GET '{k}' got status={r.status_code} body='{r.text}'"
            )

        conductor.dump_all_container_logs(dir)

    return True, "ok"
