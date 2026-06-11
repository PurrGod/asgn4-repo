import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSTestFixture
from ...utils.util import Logger


def test_missing_key_redirects_to_owning_shard(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        view = {
            "shardA": [by_id[0]],
            "shardB": [by_id[1]],
        }
        for i in range(2):
            assert fx.clients[i].send_view(view).status_code == 200, f"node {i} /view failed"
        time.sleep(1)

        chosen_key = None
        redirect = None
        for i in range(80):
            key = f"missing-edge-{i}"
            r = requests.get(
                f"{fx.clients[0].base_url}/data/{key}",
                allow_redirects=False,
                timeout=5,
            )
            if r.status_code == 307 and r.headers.get("Location", "").startswith(
                f"http://{by_id[1]['address']}/data/"
            ):
                chosen_key = key
                redirect = r
                break

        assert chosen_key is not None, "could not find a missing key owned by shardB"
        expected = f"http://{by_id[1]['address']}/data/{chosen_key}"
        assert redirect.headers.get("Location") == expected, (
            f"expected missing key redirect to {expected}, got {redirect.headers.get('Location')}"
        )

        r = requests.get(
            f"{fx.clients[1].base_url}/data/{chosen_key}",
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 404, (
            f"owning shard should decide missing key is 404, got {r.status_code}"
        )

        conductor.dump_all_container_logs(dir)

    return True, "ok"
