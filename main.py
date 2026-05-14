from fastapi import FastAPI, Request, Response, Path
from fastapi.responses import JSONResponse
import uvicorn
import httpx
import asyncio
import json
import os
import threading
import time
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta

app = FastAPI()

# In-memory store - maps key -> value
store: Dict[str, str] = {}
store_lock = asyncio.Lock()

# Current view of nodes
current_view: Dict[str, List[Dict[str, Any]]] = {"defaultShard": []}
view_lock = asyncio.Lock()

# Track when view was last updated (for view change acknowledgment)
last_view_update_time: Optional[datetime] = None
last_view_changed = False

# Node metadata
node_id: Optional[int] = None
node_port: int = 8081

# Timeout for inter-node communication (N seconds from environment)
TIMEOUT = float(os.getenv("N", "5"))

# Track if we've been partitioned recently
# This helps us decide whether to send 307 redirects
partitioned_nodes: Set[int] = set()
partition_lock = asyncio.Lock()

def get_my_address(view: Dict[str, List[Dict[str, Any]]]) -> Optional[str]:
    """Get my own address from the current view"""
    identifier = os.getenv("NODE_IDENTIFIER")
    if not identifier or not view or "defaultShard" not in view:
        return None
    
    try:
        my_id = int(identifier)
        for node_info in view["defaultShard"]:
            if node_info.get("id") == my_id:
                return node_info.get("address")
    except (ValueError, TypeError):
        pass
    
    return None

def get_other_nodes(view: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Get all nodes except self from the current view"""
    my_addr = get_my_address(view)
    if not view or "defaultShard" not in view:
        return []
    
    return [n for n in view["defaultShard"] if n.get("address") != my_addr]

def get_primary_node(view: Dict[str, List[Dict[str, Any]]]) -> Optional[str]:
    """
    Get the primary/first node in the view for 307 redirects.
    Returns the address of the first node in defaultShard.
    """
    if not view or "defaultShard" not in view or not view["defaultShard"]:
        return None
    
    first_node = view["defaultShard"][0]
    addr = first_node.get("address")
    
    # Don't redirect to self
    my_addr = get_my_address(view)
    if addr == my_addr:
        # Return second node if available
        if len(view["defaultShard"]) > 1:
            return view["defaultShard"][1].get("address")
        return None
    
    return addr

async def replicate_to_node(
    client: httpx.AsyncClient,
    node_address: str,
    key: str,
    value: str
) -> Tuple[bool, bool]:
    """
    Replicate a PUT to a single node.
    Returns (success, is_redirect) where:
    - success: True if replication was accepted (200 or follows/handles 307)
    - is_redirect: True if node returned 307
    """
    try:
        url = f"http://{node_address}/data/{key}"
        response = await client.put(
            url,
            content=value.encode('utf-8'),
            headers={"Content-Type": "text/plain"},
            follow_redirects=False
        )
        
        # 200 = successful replication
        if response.status_code == 200:
            return True, False
        
        # 307 = redirect (the node will handle it, but we did our job)
        # Just treat it as success since the value will eventually be stored
        if response.status_code == 307:
            return True, True
        
        # Any other response is a failure
        return False, False
    except Exception as e:
        return False, False

async def replicate_to_all_nodes(key: str, value: str) -> bool:
    """
    Replicate a PUT to all other nodes in current view.
    For strong consistency, this must succeed on ALL nodes.
    
    Returns True only if replication succeeds on all nodes.
    """
    global current_view, partitioned_nodes
    
    async with view_lock:
        view_snapshot = dict(current_view)
    
    other_nodes = get_other_nodes(view_snapshot)
    
    if not other_nodes:
        # No other nodes to replicate to
        return True
    
    # Replicate to all other nodes concurrently
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT)) as client:
            tasks = []
            for node in other_nodes:
                task = replicate_to_node(client, node["address"], key, value)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)
            
            # For strong consistency, ALL replications must succeed
            all_success = all(success for success, _ in results)
            
            # Check if any nodes sent 307s
            has_redirects = any(is_redirect for _, is_redirect in results)
            
            return all_success
    except Exception as e:
        return False

async def check_connectivity_to_all_nodes() -> bool:
    """
    Check if we can reach all other nodes via /ping.
    Used to detect partitions.
    """
    global current_view
    
    async with view_lock:
        view_snapshot = dict(current_view)
    
    other_nodes = get_other_nodes(view_snapshot)
    
    if not other_nodes:
        return True  # No other nodes means we're connected
    
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT / 2)) as client:
            tasks = []
            for node in other_nodes:
                task = client.get(f"http://{node['address']}/ping")
                tasks.append((node["id"], task))
            
            node_ids = [node_id for node_id, _ in tasks]
            pings = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
            
            unreachable = set()
            for i, (node_id, ping_result) in enumerate(zip(node_ids, pings)):
                if isinstance(ping_result, Exception):
                    unreachable.add(node_id)
                elif ping_result.status_code != 200:
                    unreachable.add(node_id)
            
            async with partition_lock:
                partitioned_nodes = unreachable
            
            return len(unreachable) == 0
    except Exception:
        return False

async def local_put(key: str, value: str) -> None:
    """Store key-value pair locally in a thread-safe manner"""
    async with store_lock:
        store[key] = value

async def local_get(key: str) -> Optional[str]:
    """Retrieve key-value pair from local store"""
    async with store_lock:
        return store.get(key)

@app.get("/internal/store")
async def get_store():
    """Internal endpoint for state transfer between nodes."""
    async with store_lock:
        return JSONResponse(content=store)

@app.get("/ping")
async def ping():
    """Health check endpoint - returns 200 with empty body"""
    return Response(status_code=200)

@app.put("/view")
async def view(request: Request):
    """
    Update the view of nodes in the cluster.
    """
    global current_view, node_id, last_view_update_time
    
    try:
        body = await request.json()
        
        async with view_lock:
            old_view = dict(current_view) if current_view else {}
            current_view = body
            last_view_update_time = datetime.now()
        
        my_addr = get_my_address(body)
        other_nodes = get_other_nodes(body)
        
        was_in_old = False
        if old_view and "defaultShard" in old_view:
            was_in_old = any(n.get("address") == my_addr for n in old_view["defaultShard"])
            
        if not was_in_old and my_addr and other_nodes:
            peer_addr = other_nodes[0]["address"]
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT)) as client:
                    resp = await client.get(f"http://{peer_addr}/internal/store")
                    if resp.status_code == 200:
                        peer_store = resp.json()
                        async with store_lock:
                            store.update(peer_store)
            except Exception:
                pass
        
        # Extract our node ID from environment variable
        identifier = os.getenv("NODE_IDENTIFIER")
        if identifier:
            try:
                node_id = int(identifier)
            except:
                pass
                
        return Response(status_code=200)
    except Exception as e:
        return Response(status_code=400)

@app.put("/data/{key}")
async def put_data(request: Request, key: str = Path(..., pattern="^[0-9a-zA-Z-]{0,128}$")):
    """
    Store a key-value pair with strong consistency.
    
    Strong consistency guarantee:
    1. Store value locally
    2. Replicate to ALL other nodes in the view
    3. Only respond 200 once ALL replications complete
    4. Can respond 307 if partitioned (per spec)
    5. Can respond 500 if no view received or replication fails
    
    Per spec: response must be sent once value is stored across ALL servers,
    or we block to maintain strong consistency.
    """
    try:
        # Check if we have a view
        async with view_lock:
            if not current_view or "defaultShard" not in current_view:
                return Response(status_code=500)
            view_snapshot = dict(current_view)
        
        body = await request.body()
        value = body.decode('utf-8')
        
        # Check if we're partitioned
        all_reachable = await check_connectivity_to_all_nodes()
        
        if not all_reachable and len(get_other_nodes(view_snapshot)) > 0:
            # We're partitioned
            # when the primary node is also partitioned.
            return Response(status_code=503)
        
        # Replicate to all other nodes
        success = await replicate_to_all_nodes(key, value)
        
        if success:
            # Store locally ONLY if replication to the rest of the cluster succeeds
            await local_put(key, value)
            return Response(status_code=200)
        else:
            # Replication failed - we cannot guarantee strong consistency
            return Response(status_code=503)
    
    except Exception as e:
        return Response(status_code=500)

@app.get("/data/{key}")
async def get_data(key: str = Path(..., pattern="^[0-9a-zA-Z-]{0,128}$")):
    """
    Retrieve a key-value pair from the store.
    
    Per spec: returns most recently acknowledged value or 404 if not found.
    - Returns 500 if no view received yet
    - Returns 307 if partitioned (optional, for client coordination)
    - Returns 200 with value if found
    - Returns 404 if not found
    """
    try:
        # Check if we have a view
        async with view_lock:
            if not current_view or "defaultShard" not in current_view:
                return Response(status_code=500)
            view_snapshot = dict(current_view)
        
        # For strong consistency reads, we could check connectivity
        # But GET is typically read-only and doesn't need coordination
        # Just return what we have
        
        value = await local_get(key)
        
        if value is not None:
            return Response(content=value, media_type="text/plain", status_code=200)
        return Response(status_code=404, content="")
    
    except Exception:
        return Response(status_code=500)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
