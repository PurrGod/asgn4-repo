# designed by iyyam/ajagathe/brcalcan
# written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSTestFixture
from provided_tests.utils.util import Logger


# Regression test for the "update clock when receiving sync" fix.
#
# A is primary and a value is PUT (fully acked, so B and C both receive it
# and must advance their own clocks to match). A is then removed so B becomes
# primary and a new value is PUT. Finally B is removed so C becomes primary.
# C must serve the latest value.
#
# Without the clock-advance fix, B would start from clock 0, its PUT would
# reuse a clock C has already seen, C would drop it, and the final GET on C
# would return the stale first value instead of the second.
def successive_primaries(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        fx.broadcast_view(conductor.get_view())

        client_a = fx.clients[0]
        client_b = fx.clients[1]
        client_c = fx.clients[2]

        # A (node 0) is primary -- PUT the first value
        r = client_a.put("x", "1")
        assert r.status_code == 200, f"expected put 200, got {r.status_code}"

        # remove A so B (node 1) becomes the lowest-id primary
        log("\n> CRASH NODE 0 (PRIMARY A)")
        conductor.crash_machine(0)
        view_bc = {"defaultShard": [conductor.get_node(1).get_view(), conductor.get_node(2).get_view()]}
        log(f"\n> BROADCAST VIEW {{B, C}}: {view_bc}")
        for client in (client_b, client_c):
            r = client.send_view(view_bc)
            assert r.status_code == 200, f"expected view ack 200, got {r.status_code}"

        # B is now primary -- PUT the second value
        r = client_b.put("x", "2")
        assert r.status_code == 200, f"expected put 200, got {r.status_code}"

        # sanity: the new primary returns the new value
        r = client_b.get("x")
        assert r.status_code == 200, f"expected get 200, got {r.status_code}"
        assert r.text == "2", f"expected '2' from B, got '{r.text}'"

        # remove B so C (node 2) becomes primary
        log("\n> CRASH NODE 1 (PRIMARY B)")
        conductor.crash_machine(1)
        view_c = {"defaultShard": [conductor.get_node(2).get_view()]}
        log(f"\n> BROADCAST VIEW {{C}}: {view_c}")
        r = client_c.send_view(view_c)
        assert r.status_code == 200, f"expected view ack 200, got {r.status_code}"

        # C must have the latest acked value, not the stale first one
        r = client_c.get("x")
        assert r.status_code == 200, f"expected get 200 from new primary C, got {r.status_code}"
        assert r.text == "2", f"expected '2' from C, got '{r.text}'"

        conductor.dump_all_container_logs(dir)
    return True, "ok"
