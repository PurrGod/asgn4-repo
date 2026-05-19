import requests
import time
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture
from ..utils.util import Logger


def redirect_semantics(conductor: ClusterConductor, dir, log: Logger):
    """Exercise redirect semantics: followers redirect, view change can promote follower."""
    with KVSTestFixture(conductor, dir, log, node_count=2, sync_time=5) as fx:
        fx.broadcast_view(conductor.get_view())

        c0 = fx.clients[0]
        c1 = fx.clients[1]

        # follower should redirect GETs to primary
        r = requests.get(f"{c1.base_url}/data/nope", allow_redirects=False)
        assert r.status_code == 307, f"expected 307 from follower, got {r.status_code}"

        # swap order in view so node1 becomes primary
        full = conductor.get_view()
        swapped = {"defaultShard": [full["defaultShard"][1], full["defaultShard"][0]]}
        fx.broadcast_view(swapped)
        time.sleep(0.5)

        # now node1 should no longer redirect
        r2 = c1.get("nope")
        assert r2.status_code in (200, 404), f"expected 200 or 404 from new primary, got {r2.status_code}"

    return True, "ok"
