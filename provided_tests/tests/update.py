from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSClient, KVSTestFixture
from ..utils.util import Logger


def update(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=1) as fx:
        client = fx.clients[0]

        r = client.put("test2", "original")
        assert r.status_code == 200

        r = client.put("test2", "updated")
        assert r.status_code == 200

        conductor.dump_all_container_logs(dir)

        # return score/reason
        return True, "ok"
