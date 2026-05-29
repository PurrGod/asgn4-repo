# designed by iyyam/ajagathe/brcalcan
# written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSTestFixture, REQUEST_TIMEOUT_STATUS_CODE
from provided_tests.utils.util import Logger

def dropped_write_after_promotion(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        fx.broadcast_view(conductor.get_view())

        client_a, client_b, client_c = fx.clients

        # A (node 0) is primary -- PUT v1, fully replicated to B and C
        r = client_a.put("x", "v1")
        assert r.status_code == 200, f"PUT v1: {r.status_code}"

        # isolate B (node 1); A can still reach C (node 2)
        log("\n> ISOLATE NODE 1 (B)")
        conductor.partition([1], "isob")

        # PUT v2 -- reaches C but hangs waiting on B, so it never acks (408).
        # C now holds a dirty v2 at clock=2; B is still at clock=1.
        log("\n> PUT v2 (must time out; lands on C only)")
        r = client_a.put("x", "v2", timeout=5)
        assert r.status_code == REQUEST_TIMEOUT_STATUS_CODE, (
            f"PUT v2 should have timed out (408), got {r.status_code}"
        )

        # regroup: put B and C together. This leaves A alone on the base
        # network, and A still cannot reach B, so the stuck v2 sync never
        # delivers to B.
        log("\n> REGROUP: B+C together, A isolated")
        conductor.partition([1, 2], "bc")

        # remove A from the view -> B (lowest id) becomes primary. B only
        # pushes its clock=1 data to C, so B never learns C's clock=2.
        view_bc = {
            "defaultShard": [
                conductor.get_node(1).get_view(),
                conductor.get_node(2).get_view(),
            ]
        }
        log(f"\n> VIEW {{B, C}}: {view_bc}")
        for c in (client_b, client_c):
            r = c.send_view(view_bc)
            assert r.status_code == 200, f"view {{B,C}} ack: {r.status_code}"

        # PUT v3 on the new primary B -> clock=2, collides with C's dirty
        # clock=2, so C drops it (but still 200s the sync). B acks anyway.
        log("\n> PUT v3 on B (acked, but silently dropped on C)")
        r = client_b.put("x", "v3")
        assert r.status_code == 200, f"PUT v3: {r.status_code}"

        # remove B -> C becomes primary and now serves whatever it has
        view_c = {"defaultShard": [conductor.get_node(2).get_view()]}
        log(f"\n> VIEW {{C}}: {view_c}")
        r = client_c.send_view(view_c)
        assert r.status_code == 200, f"view {{C}} ack: {r.status_code}"

        # the last acknowledged write was v3, so C must return v3
        r = client_c.get("x")
        assert r.status_code == 200, f"GET x from C: {r.status_code}"
        assert r.text == "v3", (
            f"strong consistency violated: acked 'v3' but C returned '{r.text}'"
        )

        conductor.dump_all_container_logs(dir)
    return True, "ok"
