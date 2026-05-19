import requests
import time
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture
from ..utils.util import Logger


def crash_scenarios(conductor: ClusterConductor, dir, log: Logger):
    """Test node crash: verify data persists and survivors continue operating."""
    with KVSTestFixture(conductor, dir, log, node_count=3, sync_time=5) as fx:
        fx.broadcast_view(conductor.get_view())

        c0 = fx.clients[0]
        c1 = fx.clients[1]
        c2 = fx.clients[2]

        # initial put should succeed and replicate to all
        r = c0.put("x", "v")
        assert r.status_code == 200, f"initial put failed: {r.status_code}"

        time.sleep(0.5)

        # verify all nodes have the data before crash
        for i in range(3):
            r = requests.get(f"{fx.clients[i].base_url}/internal/store")
            assert r.status_code == 200, f"internal store fetch failed on node {i}"
            store = r.json()
            assert store.get("x") == "v", f"node {i} missing value before crash"

        # crash the primary (node 0)
        conductor.crash_machine(0)
        time.sleep(0.5)

        # construct new view without crashed node and send to remaining nodes
        new_view = conductor.get_view()
        fx.send_view(1, new_view)
        fx.send_view(2, new_view)

        time.sleep(0.5)

        # verify surviving nodes still have the data
        for i in [1, 2]:
            r = requests.get(f"{fx.clients[i].base_url}/internal/store")
            assert r.status_code == 200, f"internal store fetch failed on node {i} after crash"
            store = r.json()
            assert store.get("x") == "v", f"node {i} lost data after crash"

    return True, "ok"
