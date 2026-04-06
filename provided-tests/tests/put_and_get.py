from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSClient, KVSTestFixture
from ..utils.util import Logger


def put_and_get(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=1) as fx:
        fx.broadcast_view(conductor.get_view())
        # create a cluster
        log("\n> SPAWN CLUSTER")
        conductor.spawn_cluster(node_count=1)
        client = KVSClient(conductor.node_external_endpoint(0))

        r = client.put("test1", "hello")
        assert r.status_code == 200

        r = client.get("test1")
        assert r.status_code == 200
        assert r.text == "hello"

        conductor.dump_all_container_logs(dir)

        # return score/reason
        return True, "ok"
