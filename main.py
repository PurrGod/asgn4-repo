from fastapi import FastAPI, Request, Response, Path
from fastapi.responses import JSONResponse
import uvicorn
import httpx
import asyncio
import hashlib
import bisect
import os
from typing import Dict, List, Optional, Any
import logging
import copy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Key-value store and per-key write clocks
store: Dict[str, str] = {}
store_lock = asyncio.Lock()
store_clocks: Dict[str, int] = {}

# View: { shard_id: [ {"address": "ip:port", "id": N}, ... ] }
# Empty dict means no view received yet.
current_view: Dict[str, List[Dict[str, Any]]] = {}
view_lock = asyncio.Lock()

# Per-key asyncio locks for strong-consistency PUT serialisation
put_locks: Dict[str, asyncio.Lock] = {}

# TIMEOUT:       inter-node latency budget (0.1 × N seconds)
# TIMEOUT_BULK:  budget for bulk key-transfer HTTP calls during view change (N seconds)
TIMEOUT = float(os.getenv("N", "5")) * 0.1
TIMEOUT_BULK = float(os.getenv("N", "5"))

# Virtual nodes per shard on the consistent-hash ring.
# 150 gives excellent balance: going from 4→5 shards moves ~20% of keys.
VIRTUAL_NODES = 150

# ---------------------------------------------------------------------------
# Consistent Hashing
# ---------------------------------------------------------------------------

def _ring_hash(s: str) -> int:
    """Map a string to a 128-bit integer using MD5 (fast, uniform)."""
    return int(hashlib.md5(s.encode()).hexdigest(), 16)


def build_ring(view: dict):
    """
    Build the hash ring from shard names.
    Returns (ring, sorted_positions) where:
      ring[position] = shard_id
      sorted_positions is the sorted list of all token positions
    Only shard *names* determine ring positions, not node IDs or addresses,
    so the mapping is stable when nodes are added/removed within a shard.
    """
    ring: Dict[int, str] = {}
    for shard_id in view:
        for i in range(VIRTUAL_NODES):
            pos = _ring_hash(f"{shard_id}:{i}")
            ring[pos] = shard_id
    sorted_positions = sorted(ring.keys())
    return ring, sorted_positions


def key_to_shard(key: str, ring: dict, sorted_positions: list) -> Optional[str]:
    """
    Return the shard responsible for key via clockwise lookup on the ring.
    Returns None only if the ring is empty (no shards in view).
    """
    if not sorted_positions:
        return None
    h = _ring_hash(key)
    # bisect_left: first position >= h; wrap around if past the end
    idx = bisect.bisect_left(sorted_positions, h) % len(sorted_positions)
    return ring[sorted_positions[idx]]

# ---------------------------------------------------------------------------
# Node / Shard Identity Helpers
# ---------------------------------------------------------------------------

def _my_id() -> Optional[int]:
    val = os.getenv("NODE_IDENTIFIER")
    if not val:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def get_my_shard(view: dict) -> Optional[str]:
    """Return the shard ID this node belongs to, or None if not in view."""
    my_id = _my_id()
    if my_id is None or not view:
        return None
    for shard_id, nodes in view.items():
        for node in nodes:
            if node.get("id") == my_id:
                return shard_id
    return None


def get_my_address(view: dict) -> Optional[str]:
    """Return this node's address from the view (searches all shards)."""
    my_id = _my_id()
    if my_id is None or not view:
        return None
    for nodes in view.values():
        for node in nodes:
            if node.get("id") == my_id:
                return node.get("address")
    return None


def get_shard_primary(view: dict, shard_id: str) -> Optional[str]:
    """Return the address of the primary (first) node of shard_id."""
    nodes = view.get(shard_id)
    if not nodes:
        return None
    return nodes[0].get("address")


def get_shard_primary_id(view: dict, shard_id: str) -> Optional[int]:
    """Return the node ID of the primary (first) node of shard_id."""
    nodes = view.get(shard_id)
    if not nodes:
        return None
    return nodes[0].get("id")


def get_primary_address(view: dict) -> Optional[str]:
    """Primary address of THIS node's shard."""
    my_shard = get_my_shard(view)
    if my_shard is None:
        return None
    return get_shard_primary(view, my_shard)


def get_primary_node(view: dict) -> Optional[int]:
    """Primary node ID of THIS node's shard."""
    my_shard = get_my_shard(view)
    if my_shard is None:
        return None
    return get_shard_primary_id(view, my_shard)


def is_primary(view: dict) -> bool:
    """True iff this node is the primary of its shard."""
    my_addr = get_my_address(view)
    if my_addr is None:
        return False
    return my_addr == get_primary_address(view)


def get_other_nodes(view: dict) -> List[Dict[str, Any]]:
    """Other nodes in this node's shard (all shards work, not just defaultShard)."""
    my_addr = get_my_address(view)
    my_shard = get_my_shard(view)
    if my_shard is None or not view:
        return []
    return [n for n in view.get(my_shard, []) if n.get("address") != my_addr]


def get_node_ids(view: dict) -> set:
    """All node IDs across all shards."""
    ids: set = set()
    for nodes in view.values():
        for n in nodes:
            nid = n.get("id")
            if nid is not None:
                ids.add(nid)
    return ids


def _node_to_shard_map(view: dict) -> dict:
    """Maps node_id → shard_id for every node in the view."""
    result: Dict[int, str] = {}
    for shard_id, nodes in view.items():
        for n in nodes:
            nid = n.get("id")
            if nid is not None:
                result[nid] = shard_id
    return result

# ---------------------------------------------------------------------------
# Intra-shard Replication Helpers
# ---------------------------------------------------------------------------

async def replicate_to_node(
    client: httpx.AsyncClient, node_address: str, key: str, value: str
) -> bool:
    try:
        resp = await client.put(
            f"http://{node_address}/internal/data/{key}",
            content=value.encode("utf-8"),
            headers={"Content-Type": "text/plain"},
            follow_redirects=False,
        )
        return resp.status_code == 200
    except Exception:
        return False


async def local_put(key: str, value: str) -> None:
    async with store_lock:
        store[key] = value
        store_clocks[key] = store_clocks.get(key, 0) + 1


async def local_get(key: str) -> Optional[str]:
    async with store_lock:
        return store.get(key)


async def process_put(key: str, value: str) -> Response:
    """
    Strong-consistency PUT within this node's shard.
    Runs as a background task so the client can disconnect while the server
    keeps retrying until all shard peers acknowledge.
    """
    async with put_locks[key]:
        try:
            async with view_lock:
                if not current_view:
                    return Response(status_code=500)
                view_snap = copy.deepcopy(current_view)

            # If we somehow ended up not being primary, redirect
            if not is_primary(view_snap):
                primary = get_primary_address(view_snap)
                if primary:
                    return Response(
                        status_code=307,
                        headers={"Location": f"http://{primary}/data/{key}"},
                    )
                return Response(status_code=500)

            # Retry loop: replicate to all shard peers before acknowledging
            while True:
                async with view_lock:
                    if not current_view:
                        return Response(status_code=500)
                    view_snap = copy.deepcopy(current_view)

                if not is_primary(view_snap):
                    primary = get_primary_address(view_snap)
                    if primary:
                        return Response(
                            status_code=307,
                            headers={"Location": f"http://{primary}/data/{key}"},
                        )
                    return Response(status_code=500)

                others = get_other_nodes(view_snap)
                if not others:
                    break  # single-node shard: no replication needed

                try:
                    async with httpx.AsyncClient(
                        timeout=httpx.Timeout(TIMEOUT)
                    ) as client:
                        results = await asyncio.gather(
                            *[
                                replicate_to_node(client, n["address"], key, value)
                                for n in others
                            ]
                        )
                    if all(results):
                        break
                except Exception:
                    pass

            await local_put(key, value)
            return Response(status_code=200)

        except Exception:
            return Response(status_code=500)

# ---------------------------------------------------------------------------
# Internal Endpoints  (never triggered by external clients)
# ---------------------------------------------------------------------------

@app.put("/internal/data/{key}")
async def internal_put_data(request: Request, key: str):
    """Intra-shard replication: persist locally, never re-replicate."""
    try:
        body = await request.body()
        await local_put(key, body.decode("utf-8"))
        return Response(status_code=200)
    except Exception:
        return Response(status_code=500)


@app.post("/internal/batch")
async def internal_batch_put(request: Request):
    """
    Bulk key import during cross-shard redistribution.
    Body: {"key1": "val1", "key2": "val2", ...}
    """
    try:
        kv: Dict[str, str] = await request.json()
        async with store_lock:
            for k, v in kv.items():
                store[k] = v
                store_clocks[k] = store_clocks.get(k, 0) + 1
        return Response(status_code=200)
    except Exception:
        return Response(status_code=500)


@app.get("/internal/store")
async def get_internal_store():
    """Return the full local key-value store (used for peer state transfer)."""
    async with store_lock:
        return JSONResponse(content=dict(store))


@app.get("/internal/clocks")
async def get_internal_clocks():
    """Return the local write-clock map (used for intra-shard primary-change sync)."""
    async with store_lock:
        return JSONResponse(content=dict(store_clocks))


@app.post("/internal/values")
async def get_internal_values(request: Request):
    """Return values for a specified list of keys."""
    try:
        body = await request.json()
        async with store_lock:
            return JSONResponse(
                content={k: store[k] for k in body.get("keys", []) if k in store}
            )
    except Exception:
        return Response(status_code=500)

# ---------------------------------------------------------------------------
# Public Endpoints
# ---------------------------------------------------------------------------

@app.get("/ping")
async def ping():
    return Response(status_code=200)


@app.put("/view")
async def view_change(request: Request):
    """
    Accept a new cluster view and perform all necessary data migrations.

    Three sync cases, evaluated in order:
      A. This node is new or moved to a different shard → pull full store from a
         peer already in the new shard.
      B. Shard names changed (shards added/removed) → each shard's primary pushes
         keys that now belong to other shards to ALL nodes in the destination shard,
         then deletes them locally.  Pushing to all destination nodes (not just the
         primary) avoids a second fan-out step and satisfies the spec requirement
         that all shard members are strongly consistent once all nodes return 200.
      C. Primary changed within this node's shard → follower pulls divergent keys
         from the new primary via clock diff.
    """
    global current_view
    try:
        new_view: dict = await request.json()
    except Exception:
        return Response(status_code=400)

    try:
        async with view_lock:
            old_view = copy.deepcopy(current_view) if current_view else {}
            current_view = new_view

        # ── Early return: only IPs / node-order within shards changed ──────
        # node-to-shard assignment is identical ⟹ no data movement needed.
        if _node_to_shard_map(old_view) == _node_to_shard_map(new_view):
            return Response(status_code=200)

        old_shard_names = frozenset(old_view.keys())
        new_shard_names = frozenset(new_view.keys())
        shard_structure_changed = (old_shard_names != new_shard_names)

        my_id = _my_id()
        my_addr = get_my_address(new_view)
        new_my_shard = get_my_shard(new_view)
        old_my_shard = get_my_shard(old_view)
        was_in_old = my_id is not None and my_id in get_node_ids(old_view)

        # ── CASE A: New node or shard reassignment ───────────────────────
        # Pull the current shard store from any peer that was already in the
        # cluster (to avoid pulling from another fresh joinee).
        if new_my_shard is not None and (
            not was_in_old or old_my_shard != new_my_shard
        ):
            old_ids = get_node_ids(old_view)
            # Prefer peers who existed before this view change
            all_peers = [
                n for n in new_view.get(new_my_shard, [])
                if n.get("address") != my_addr
            ]
            existing_peers = [n for n in all_peers if n.get("id") in old_ids]
            pull_peers = existing_peers if existing_peers else all_peers
            if pull_peers:
                peer_addr = pull_peers[0]["address"]
                try:
                    async with httpx.AsyncClient(
                        timeout=httpx.Timeout(TIMEOUT_BULK)
                    ) as client:
                        resp = await client.get(
                            f"http://{peer_addr}/internal/store"
                        )
                        if resp.status_code == 200:
                            async with store_lock:
                                store.update(resp.json())
                except Exception:
                    pass

        # ── CASE B: Cross-shard key redistribution ───────────────────────
        # Only the primary of each shard performs the push (followers receive
        # the batch directly, avoiding double-sends).
        if shard_structure_changed and new_my_shard is not None and is_primary(new_view):
            new_ring, new_sorted = build_ring(new_view)

            # Collect keys that must move to other shards
            keys_to_push: Dict[str, Dict[str, str]] = {}  # dest_shard → {k: v}
            keys_to_delete: List[str] = []

            async with store_lock:
                for k, v in store.items():
                    dest = key_to_shard(k, new_ring, new_sorted)
                    if dest != new_my_shard:
                        keys_to_push.setdefault(dest, {})[k] = v
                        keys_to_delete.append(k)

            if keys_to_push:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(TIMEOUT_BULK)
                ) as client:
                    for dest_shard, kv_batch in keys_to_push.items():
                        # Push to ALL nodes in the destination shard so that
                        # every replica is up-to-date before we return 200.
                        for dest_node in new_view.get(dest_shard, []):
                            dest_addr = dest_node.get("address")
                            if dest_addr:
                                try:
                                    await client.post(
                                        f"http://{dest_addr}/internal/batch",
                                        json=kv_batch,
                                    )
                                except Exception:
                                    pass

            # Remove pushed keys from our local store
            if keys_to_delete:
                async with store_lock:
                    for k in keys_to_delete:
                        store.pop(k, None)
                        store_clocks.pop(k, None)

        # ── CASE C: Intra-shard primary reconciliation ────────────────────
        # If this node is a follower and the primary changed, pull any
        # divergent values from the new primary via clock diff.
        new_primary_addr = get_primary_address(new_view)
        old_primary_id = (
            get_shard_primary_id(old_view, old_my_shard) if old_my_shard else None
        )
        new_primary_id = (
            get_shard_primary_id(new_view, new_my_shard) if new_my_shard else None
        )

        if (
            old_primary_id != new_primary_id
            and my_addr is not None
            and my_addr != new_primary_addr
            and new_primary_addr is not None
        ):
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(TIMEOUT)
                ) as client:
                    resp = await client.get(
                        f"http://{new_primary_addr}/internal/clocks"
                    )
                    if resp.status_code == 200:
                        primary_clocks = resp.json()
                        async with store_lock:
                            diff_keys = [
                                k
                                for k in store_clocks.keys() & primary_clocks.keys()
                                if store_clocks[k] != primary_clocks[k]
                            ]
                        if diff_keys:
                            resp2 = await client.post(
                                f"http://{new_primary_addr}/internal/values",
                                json={"keys": diff_keys},
                            )
                            if resp2.status_code == 200:
                                async with store_lock:
                                    store.update(resp2.json())
            except Exception:
                pass

        return Response(status_code=200)

    except Exception:
        return Response(status_code=400)


@app.put("/data/{key}")
async def put_data(
    request: Request,
    key: str = Path(..., pattern="^[0-9a-zA-Z-]{0,128}$"),
):
    """
    Store key=value with strong consistency.

    Routing (two layers):
      1. Cross-shard: if the responsible shard ≠ our shard → 307 to that shard's primary.
         This also covers excluded nodes (my_shard is None): every key's responsible
         shard differs from None, so excluded nodes always redirect.
      2. Intra-shard: process_put handles primary/follower logic within our shard.

    No-view fallback: if no view has been received yet, behave as a standalone
    single-node primary (allows tests that skip the view broadcast to work).
    """
    try:
        async with view_lock:
            view_snap = copy.deepcopy(current_view) if current_view else None

        # ── No view received: standalone single-node mode ─────────────────
        if view_snap is None:
            if key not in put_locks:
                put_locks[key] = asyncio.Lock()
            value = (await request.body()).decode("utf-8")
            await local_put(key, value)
            return Response(status_code=200)

        # ── Cross-shard routing (my_shard may be None for excluded nodes) ─
        my_shard = get_my_shard(view_snap)
        ring, sorted_pos = build_ring(view_snap)
        responsible = key_to_shard(key, ring, sorted_pos)

        if responsible != my_shard:
            primary = get_shard_primary(view_snap, responsible)
            if primary is None:
                return Response(status_code=500)
            return Response(
                status_code=307,
                headers={"Location": f"http://{primary}/data/{key}"},
            )

        # Key belongs to our shard: use background-task strong-consistency protocol
        if key not in put_locks:
            put_locks[key] = asyncio.Lock()
        value = (await request.body()).decode("utf-8")
        task = asyncio.create_task(process_put(key, value))
        resp = await asyncio.shield(task)
        return resp

    except Exception:
        return Response(status_code=500)


@app.get("/data/{key}")
async def get_data(key: str = Path(..., pattern="^[0-9a-zA-Z-]{0,128}$")):
    """
    Retrieve the most recently acknowledged value for key.

    Routing (two layers):
      1. Cross-shard: if the responsible shard ≠ our shard → 307 to that shard's primary.
         Excluded nodes (my_shard is None) always redirect this way.
      2. Intra-shard: only the shard primary serves reads; followers return 307.

    No-view fallback: serve directly from local store.
    """
    try:
        async with view_lock:
            view_snap = copy.deepcopy(current_view) if current_view else None

        # ── No view received: standalone single-node mode ─────────────────
        if view_snap is None:
            value = await local_get(key)
            if value is None:
                return Response(status_code=404, content="")
            return Response(content=value, media_type="text/plain", status_code=200)

        # ── Cross-shard routing (my_shard may be None for excluded nodes) ─
        my_shard = get_my_shard(view_snap)
        ring, sorted_pos = build_ring(view_snap)
        responsible = key_to_shard(key, ring, sorted_pos)

        if responsible != my_shard:
            primary = get_shard_primary(view_snap, responsible)
            if primary is None:
                return Response(status_code=500)
            return Response(
                status_code=307,
                headers={"Location": f"http://{primary}/data/{key}"},
            )

        if not is_primary(view_snap):
            primary = get_primary_address(view_snap)
            if primary:
                return Response(
                    status_code=307,
                    headers={"Location": f"http://{primary}/data/{key}"},
                )
            return Response(status_code=500)

        value = await local_get(key)
        if value is None:
            return Response(status_code=404, content="")
        return Response(content=value, media_type="text/plain", status_code=200)

    except Exception:
        return Response(status_code=500)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
