import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSClient, KVSTestFixture
from ...utils.util import Logger


def test_view_accepts_arbitrary_shard_names(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=1) as fx:
        full = conductor.get_view()
        node = full["defaultShard"][0]

        view = {"alpha": [node]}
        c = fx.clients[0]
        r = requests.put(f"{c.base_url}/view", json=view, timeout=10)
        assert r.status_code == 200, (
            f"expected 200 for a spec-format view with a non-defaultShard name, "
            f"got {r.status_code} (server likely hardcodes data['defaultShard'])"
        )
        assert r.text == "", f"expected empty body, got '{r.text}'"
        time.sleep(1)

        r = requests.put(
            f"{c.base_url}/data/k",
            data="v",
            headers={"Content-Type": "text/plain"},
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 200, f"PUT after arbitrary-name view got {r.status_code}"

        r = requests.get(f"{c.base_url}/data/k", allow_redirects=False, timeout=5)
        assert r.status_code == 200, f"GET after arbitrary-name view got {r.status_code}"
        assert r.text == "v", f"expected 'v', got '{r.text}'"

        conductor.dump_all_container_logs(dir)

    return True, "ok"