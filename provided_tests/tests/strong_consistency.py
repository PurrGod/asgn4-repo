import requests
from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSTestFixture
from ..utils.util import Logger


def put_is_immediately_global(conductor: ClusterConductor, dir, log: Logger):
  """Strong consistency: Once PUT is acknowledged, all nodes must return it."""
  with KVSTestFixture(conductor, dir, log, node_count=3, sync_time=5) as fx:
    fx.broadcast_view(conductor.get_view())

    c0 = fx.clients[0]
    c1 = fx.clients[1]
    c2 = fx.clients[2]
    primary = conductor.nodes[0].ip + ":8081"
    
    # use client 0 (Node 0 is primary)
    r0 = c0.put("x", "v1")
    assert r0.status_code == 200, f"Primary failed PUT: {r0.status_code}"

    # immediate GET from all nodes
    r0 = c0.get("x")
    assert r0.status_code == 200, f"Primary returned the wrong code: {r0.status_code}"

    r1 = requests.get(f"{c1.base_url}/data/x", allow_redirects=False)
    assert r1.status_code == 307, f"Node 1 should have redirected request to primary"
    assert primary in r1.headers["Location"], f"Primary was not routed to in the Location header, {r1.headers["Location"]}"

    r2 = requests.get(f"{c2.base_url}/data/x", allow_redirects=False) 
    assert r2.status_code == 307, f"Node 2 should have redirected request to primary"
    assert primary in r2.headers["Location"], f"Primary was not routed to in the Location header, {r2.headers["Location"]}"

    r_final = c0.get("x")
    assert r_final.status_code == 200, f"Primary returned the wrong code: {r_final.status_code}"
    assert r_final.text == "v1", f"Primary returned stale data"

  return True, "ok"
