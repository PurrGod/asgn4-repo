import requests
import time
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture
from ..utils.util import Logger


def basic_view_change_under_partition(conductor: ClusterConductor, dir, log: Logger):
    """Test /view handling: malformed JSON and proper view acceptance."""
    with KVSTestFixture(conductor, dir, log, node_count=2, sync_time=5) as fx:
        
        # FROM TA ("full test")
        # def basic_view_change_under_partition(conductor: ClusterConductor, fx: KvsFixture):
        #     node1, node2 = nodes = conductor.spawn_cluster(node_count=2)
        #     mc = fx.create_client(name="tester")
        #     mc.broadcast_view([node1, node2])
        #     conductor.isolate(node1)
        #     mc.broadcast_view(nodes)
        #     return True, "ok"

        fx.broadcast_view(conductor.get_view())

        # partition each node into its own network
        conductor.partition([0], "p0")
        conductor.partition([1], "p1")

        fx.broadcast_view(conductor.get_view())

    return True, "ok"
