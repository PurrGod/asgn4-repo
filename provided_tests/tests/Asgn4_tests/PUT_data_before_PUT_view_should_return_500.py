import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSTestFixture
from ...utils.util import Logger


def test_put_data_before_view_returns_500(
    conductor: ClusterConductor, dir, log: Logger
):
    with KVSTestFixture(conductor, dir, log, node_count=1) as fx:
        r = requests.put(
            f"{fx.clients[0].base_url}/data/apple",
            data="red",
            headers={"Content-Type": "text/plain"},
            allow_redirects=False,
            timeout=5,
        )

        assert r.status_code == 500, (
            f"PUT /data before /view should return 500, got {r.status_code}"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"