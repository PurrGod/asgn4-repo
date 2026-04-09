from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSClient, KVSTestFixture
from ..utils.util import Logger


def put_and_get(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=1) as fx:
        client = fx.clients[0]

        r = client.put("test1", "hello")
        assert r.status_code == 200, (
            f"expected put status code to be 200, got {r.status_code}"
        )

        r = client.get("test1")
        assert r.status_code == 200, (
            f"expected get status code to be 200, got {r.status_code}"
        )
        assert r.text == "hello"

        conductor.dump_all_container_logs(dir)

        # return score/reason
    return True, "ok"
