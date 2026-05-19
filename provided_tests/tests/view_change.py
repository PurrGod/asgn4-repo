import requests
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture
from ..utils.util import Logger


def view_change(conductor: ClusterConductor, dir, log: Logger):
    """Test /view handling: malformed JSON and proper view acceptance."""
    with KVSTestFixture(conductor, dir, log, node_count=1) as fx:
        client = fx.clients[0]

        # malformed JSON should return 400
        r = requests.put(f"{client.base_url}/view", data="not-json", headers={"Content-Type": "application/json"})
        assert r.status_code == 400, f"expected 400 for malformed view, got {r.status_code}"

        # proper view should be accepted via broadcast (KVSTestFixture asserts 200)
        fx.broadcast_view(conductor.get_view())

    return True, "ok"
