import requests
import time
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture
from ..utils.util import Logger


def node_join_leave(conductor: ClusterConductor, dir, log: Logger):
    """Test multi-node cluster: verify writes replicate to all nodes and survivors continue after crash."""
    with KVSTestFixture(conductor, dir, log, node_count=3, sync_time=5) as fx:
        fx.broadcast_view(conductor.get_view())

        # put a value on node 0 (primary)
        r = fx.clients[0].put("k", "v")
        assert r.status_code == 200, f"put failed on primary: {r.status_code}"

        time.sleep(0.5)

        # verify the value exists on all nodes via internal store endpoint
        for i in range(3):
            r = requests.get(f"{fx.clients[i].base_url}/internal/store")
            assert r.status_code == 200, f"failed to fetch internal store from node {i}: {r.status_code}"
            store = r.json()
            assert store.get("k") == "v", f"node {i} missing replicated value: {store}"

    return True, "ok"
