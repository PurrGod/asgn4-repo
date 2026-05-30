from fastapi import FastAPI, Request, Response, Path
from fastapi.responses import JSONResponse
import uvicorn
import httpx
import asyncio
import os
from typing import Dict, List, Optional, Any
import logging
import copy

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Create a logger instance
logger = logging.getLogger(__name__)

app = FastAPI()

# In-memory store - maps key -> value
store: Dict[str, str] = {}
store_lock = asyncio.Lock()

store_clocks: Dict[str, int] = {}

# Current view of nodes
current_view: Dict[str, List[Dict[str, Any]]] = {"defaultShard": []}
view_lock = asyncio.Lock()

put_locks: Dict[str, asyncio.Lock] = {}

# Timeout for inter-node communication (N seconds from environment)
TIMEOUT = float(os.getenv("N", "5")) * 0.1 # 0.1 taken from latency spec

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

def get_node_ids(view: Dict[str, List[Dict[str, Any]]]) -> set[int]:
    """Get a set of node ids in the view"""
    if not view or "defaultShard" not in view:
        return set()
    
    return set(n.get("id") for n in view["defaultShard"])

def get_primary_address(view: Dict[str, List[Dict[str, Any]]]) -> Optional[str]:
    """
    Get the primary/first node in the view.
    Returns the address of the first node in defaultShard.
    """
    if not view or "defaultShard" not in view or not view["defaultShard"]:
        return None
    
    return view["defaultShard"][0].get("address")

def get_primary_node(view: Dict[str, List[Dict[str, Any]]]) -> Optional[int]:
    """
    Get the primary/first node in the view.
    Returns the address of the first node in defaultShard.
    """
    if not view or "defaultShard" not in view or not view["defaultShard"]:
        return None
    
    return view["defaultShard"][0].get("id")

def is_primary(view: Dict[str, List[Dict[str, Any]]]) -> bool:
    """
    Returns whether this node is the primary
    """
    return get_my_address(view) == get_primary_address(view)

async def replicate_to_node(
    client: httpx.AsyncClient,
    node_address: str,
    key: str,
    value: str
) -> bool:
    """
    Replicate a PUT to a single node.
    Returns a bool indicating whether replication was successful
    """
    try:
        url = f"http://{node_address}/internal/data/{key}"
        response = await client.put(
            url,
            content=value.encode('utf-8'),
            headers={"Content-Type": "text/plain"},
            follow_redirects=False
        )
        
        # 200 = successful replication
        if response.status_code == 200:
            return True
        
        return False # Any other response is a failure
    
    except Exception as e:
        return False

async def process_put(key: str, value: str) -> Response:
    async with put_locks[key]:
        try:
            # Check if we have a view
            async with view_lock:
                if not current_view or "defaultShard" not in current_view:
                    return Response(status_code=500) # No view, return 500
                view_snapshot = copy.deepcopy(current_view)
            
            # Check if we are at primary, if not redirect to primary
            if (not is_primary(view_snapshot)):
                return Response(
                    status_code=307, 
                    headers={"Location": f"http://{get_primary_address(view_snapshot)}/data/{key}"}
                )

            # Continue trying to send to replicants until success
            while(True):
                # Check if we have a view
                async with view_lock:
                    if not current_view or "defaultShard" not in current_view:
                        return Response(status_code=500) # No view, return 500
                    view_snapshot = copy.deepcopy(current_view)

                # Check if we are at primary, if not redirect to primary
                if (not is_primary(view_snapshot)):
                    return Response(
                        status_code=307, 
                        headers={"Location": f"http://{get_primary_address(view_snapshot)}/data/{key}"}
                    )
                
                other_nodes = get_other_nodes(view_snapshot)
                
                if not other_nodes:
                    break
                
                # Replicate to all other nodes concurrently
                try:
                    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT)) as client:
                        tasks = []
                        for node in other_nodes:
                            task = replicate_to_node(client, node["address"], key, value)
                            tasks.append(task)
                        
                        results = await asyncio.gather(*tasks)
                        
                        # For strong consistency, ALL replications must succeed
                        if all(success for success in results):
                            break

                except Exception as e:
                    pass
            
            # Store locally
            await local_put(key, value)
            return Response(status_code=200)
        
        except Exception as e:
            return Response(status_code=500)

async def local_put(key: str, value: str) -> None:
    """Store key-value pair locally in a thread-safe manner"""
    async with store_lock:
        store[key] = value
        store_clocks[key] = store_clocks.get(key, 0) + 1

async def local_get(key: str) -> Optional[str]:
    """Retrieve key-value pair from local store"""
    async with store_lock:
        return store.get(key)

@app.put("/internal/data/{key}")
async def internal_put_data(request: Request, key: str):
    """Internal endpoint for replication. ONLY saves locally, never replicates further."""
    try:
        body = await request.body()
        value = body.decode('utf-8')
        await local_put(key, value)
        return Response(status_code=200)
    except Exception:
        return Response(status_code=500)

@app.get("/internal/store")
async def get_store():
    """Internal endpoint for state transfer between nodes."""
    try:
        async with store_lock:
            return JSONResponse(content=store)
    except Exception as e:
        return Response(status_code=500)
    
@app.post("/internal/values")
async def get_values(request: Request):
    """Internal endpoint for state transfer between nodes."""
    try:
        body = await request.json()
        async with store_lock:
            keys = body.get("keys", [])
            resp = {k: store[k] for k in keys}
            return JSONResponse(content=resp)
    except Exception as e:
        return Response(status_code=500)
    
@app.get("/internal/clocks")
async def get_store_clocks():
    """Internal endpoint to get clocks from primary"""
    try:
        async with store_lock:
            return JSONResponse(content=store_clocks)
    except Exception as e:
        return Response(status_code=500)

@app.get("/ping")
async def ping():
    """Health check endpoint - returns 200 with empty body"""
    try:
        return Response(status_code=200)
    except Exception as e:
        return Response(status_code=500)

@app.put("/view")
async def view(request: Request):
    """
    Update the view of nodes in the cluster.
    """
    global current_view
    
    try:
        body = await request.json()
        
        async with view_lock:
            old_view = copy.deepcopy(current_view) if current_view else {}
            current_view = body

        # If only ip addresses changed then no need for anyone to update values
        node_ids_old = get_node_ids(old_view)
        if (node_ids_old == get_node_ids(current_view)):
            return Response(status_code=200)
        
        my_addr = get_my_address(current_view)

        # Determine if this node was already in the cluster before this view change
        was_in_old = False
        if old_view and "defaultShard" in old_view:
            old_ids = {n.get("id") for n in old_view["defaultShard"]}
            identifier = os.getenv("NODE_IDENTIFIER")
            if identifier:
                try:
                    was_in_old = int(identifier) in old_ids
                except (ValueError, TypeError):
                    pass

        # If we are a new node, download the full store from any available peer
        if not was_in_old and my_addr:
            available_peers = get_other_nodes(current_view)
            if available_peers:
                peer_addr = available_peers[0]["address"]
                try:
                    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT)) as client:
                        resp = await client.get(f"http://{peer_addr}/internal/store")
                        if resp.status_code == 200:
                            peer_store = resp.json()
                            async with store_lock:
                                store.update(peer_store)
                except Exception:
                    pass
        
        # If there is a new primary and we aren't it, update our values
        primary_addr = get_primary_address(current_view)

        if get_primary_node(old_view) != get_primary_node(current_view) and my_addr != primary_addr:
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT)) as client:
                    resp = await client.get(f"http://{primary_addr}/internal/clocks")
                    if resp.status_code == 200:
                        primary_clocks = resp.json()
                        async with store_lock:
                            different_clocks = [k for k in store_clocks.keys() & primary_clocks.keys() if store_clocks[k] != primary_clocks[k]]
                        payload = { "keys": different_clocks }
                        resp = await client.post(f"http://{primary_addr}/internal/values", json=payload)
                        if resp.status_code == 200:
                            updated_dict = resp.json()
                            async with store_lock:
                                store.update(updated_dict)
            except Exception as e:
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
    4. Can respond 307 if not primary (per spec)
    5. Can respond 500 if no view received or replication fails
    
    Per spec: response must be sent once value is stored across ALL servers,
    or we block to maintain strong consistency.
    """
    # Do put work in a detached coroutine so even if client times out we still finish work
    try:
        if key not in put_locks:
            put_locks[key] = asyncio.Lock()

        value = (await request.body()).decode('utf-8')
        task = asyncio.create_task(process_put(key, value))
        resp = await asyncio.shield(task)
        return resp
    except Exception as e:
        return Response(status_code=500) # Something went wrong, return 500

@app.get("/data/{key}")
async def get_data(key: str = Path(..., pattern="^[0-9a-zA-Z-]{0,128}$")):
    """
    Retrieve a key-value pair from the store.
    
    Per spec: returns most recently acknowledged value or 404 if not found.
    - Returns 500 if no view received yet
    - Returns 307 redirecting to primary if not primary
    - Returns 200 with value if found
    - Returns 404 if not found
    """
    try:
        # Check if we have a view
        async with view_lock:
            if not current_view or "defaultShard" not in current_view:
                return Response(status_code=500) # No view, return 500
            view_snapshot = copy.deepcopy(current_view)
        
        # Check if we are at primary, if not redirect to primary
        if (not is_primary(view_snapshot)):
            return Response(
                status_code=307, 
                headers={"Location": f"http://{get_primary_address(view_snapshot)}/data/{key}"}
            )

        value = await local_get(key)
        
        if value is None:
            return Response(status_code=404, content="")
        
        return Response(content=value, media_type="text/plain", status_code=200)
    
    except Exception:
        return Response(status_code=500)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
