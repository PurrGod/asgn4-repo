# designed by iyyam/ajagathe/brcalcan
# Written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSTestFixture
from provided_tests.utils.util import Logger
from ..utils.redirect_client import RedirectAwareClient


def single_node_view(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        clients = [RedirectAwareClient(c.base_url, conductor) for c in fx.clients]

        # Initial 3-node view; primary = node 0. Seed some data while all
        # three nodes are in the view so each carries it forward.
        fx.broadcast_view(conductor.get_view())
        for i in range(3):
            r = clients[0].put(f"seed{i}", f"val{i}")
            assert r.status_code == 200, f"seed put {i}: {r.status_code}"

        # Cycle through three singleton views: {0} -> {1} -> {2}. Under each,
        # the sole node is primary, the sync loop has no peers to talk to,
        # and PUTs/GETs must behave like a single-node store.
        #
        # Note: keys written under one singleton view (e.g. k-1 written under
        # {1}) are not expected to be visible to nodes outside that view, per
        # the spec carveout: "the view change frees the service from ensuring
        # strong consistency with nodes no longer in the view." So we assert
        # only within each view, using distinct keys per view.
        for target in (0, 1, 2):
            log(f"\n> SHRINK VIEW TO {{node {target}}}")
            node = conductor.get_node(target)
            singleton = {"defaultShard": [node.get_view()]}
            fx.broadcast_view(singleton)

            # Tight timeout: catches a regression where the primary still
            # waits on backups that are no longer in its view.
            log(f"\n> PUT VIA NODE {target} (must ack quickly)")
            r = clients[target].put(f"k-{target}", f"from-{target}", timeout=2)
            assert r.status_code == 200, (
                f"put under view {{{target}}}: {r.status_code}"
            )

            # Read-your-write from the in-view node.
            r = clients[target].get(f"k-{target}")
            assert r.status_code == 200, (
                f"get k-{target} under view {{{target}}}: {r.status_code}"
            )
            assert r.text == f"from-{target}", (
                f"node {target} k-{target}: got '{r.text}'"
            )

            # The seed keys (written under the initial 3-node view) were on
            # every node when this node carried them through the shrink, so
            # the singleton node must still serve them.
            for i in range(3):
                r = clients[target].get(f"seed{i}")
                assert r.status_code == 200, (
                    f"get seed{i} under view {{{target}}}: {r.status_code}"
                )
                assert r.text == f"val{i}", (
                    f"node {target} seed{i}: got '{r.text}'"
                )

            # GETs to nodes NOT in the view must still resolve to the right
            # value. Our implementation 307s to the in-view primary; the
            # RedirectAwareClient follows the redirect, so we just assert a
            # 200 + correct body. A 500 (also spec-legal) would fail here
            # because the redirect chain wouldn't terminate at the primary.
            log(f"\n> GETs FROM EXCLUDED NODES (must 307 to node {target})")
            for excluded in range(3):
                if excluded == target:
                    continue

                r = clients[excluded].get(f"k-{target}")
                assert r.status_code == 200, (
                    f"get k-{target} from excluded node {excluded}: "
                    f"{r.status_code}"
                )
                assert r.text == f"from-{target}", (
                    f"excluded node {excluded} via 307 -> k-{target}: "
                    f"got '{r.text}'"
                )

                for i in range(3):
                    r = clients[excluded].get(f"seed{i}")
                    assert r.status_code == 200, (
                        f"get seed{i} from excluded node {excluded}: "
                        f"{r.status_code}"
                    )
                    assert r.text == f"val{i}", (
                        f"excluded node {excluded} via 307 -> seed{i}: "
                        f"got '{r.text}'"
                    )

        conductor.dump_all_container_logs(dir)
    return True, "ok"
