import time
import requests

from ...utils.containers import ClusterConductor
from ...utils.kvs_api import KVSTestFixture
from ...utils.util import Logger


def _shard_primary_ids(view):
    return [min(n["id"] for n in nodes) for nodes in view.values()]


def _routed_put(fx, primary_ids, key, value):
    last = None
    for pid in primary_ids:
        r = requests.put(
            f"{fx.clients[pid].base_url}/data/{key}",
            data=value,
            headers={"Content-Type": "text/plain"},
            allow_redirects=False,
            timeout=5,
        )
        last = r
        if r.status_code == 200:
            return pid, r
    return None, last


def test_old_owner_redirects_after_rekey(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=5) as fx:
        full = conductor.get_view()
        by_id = {n["id"]: n for n in full["defaultShard"]}

        before_view = {
            "s0": [by_id[0]],
            "s1": [by_id[1]],
            "s2": [by_id[2]],
            "s3": [by_id[3]],
        }
        for i in range(4):
            assert fx.clients[i].send_view(before_view).status_code == 200
        time.sleep(1)

        primaries_before = _shard_primary_ids(before_view)
        before_owner = {}
        for i in range(150):
            key = f"move-edge-{i}"
            owner, _ = _routed_put(fx, primaries_before, key, f"value-{i}")
            assert owner is not None, f"PUT '{key}' not accepted by any shard"
            before_owner[key] = owner

        after_view = dict(before_view)
        after_view["s4"] = [by_id[4]]
        for i in range(5):
            assert fx.clients[i].send_view(after_view).status_code == 200
        time.sleep(2)

        chosen_key = None
        old_owner = None
        for key, previous_owner in before_owner.items():
            if previous_owner == 4:
                continue
            r = requests.get(
                f"{fx.clients[4].base_url}/data/{key}",
                allow_redirects=False,
                timeout=5,
            )
            if r.status_code == 200:
                chosen_key = key
                old_owner = previous_owner
                break

        assert chosen_key is not None, "could not find a key that moved to the new shard"

        r = requests.get(
            f"{fx.clients[old_owner].base_url}/data/{chosen_key}",
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 307, (
            f"old owner should redirect moved key, got {r.status_code} with body '{r.text}'"
        )
        expected = f"http://{by_id[4]['address']}/data/{chosen_key}"
        assert r.headers.get("Location") == expected, (
            f"expected redirect to new shard {expected}, got {r.headers.get('Location')}"
        )

        r = requests.get(
            f"{fx.clients[4].base_url}/data/{chosen_key}",
            allow_redirects=False,
            timeout=5,
        )
        assert r.status_code == 200, f"new shard should serve moved key, got {r.status_code}"

        conductor.dump_all_container_logs(dir)

    return True, "ok"
