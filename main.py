from fastapi import FastAPI, Request, Response, Path
import uvicorn

app = FastAPI()
store = {}

@app.get("/ping")
async def ping():
    return Response(status_code=200)

@app.put("/view")
async def view(request: Request):
    try:
        _ = await request.json()
        return Response(status_code=200)
    except:
        return Response(status_code=400) 

@app.put("/data/{key}")
async def put_data(request: Request, key: str = Path(..., pattern="^[0-9a-zA-Z-]{0,128}$")):
    body = await request.body()
    store[key] = body.decode('ascii')
    return Response(status_code=200)

@app.get("/data/{key}")
async def get_data(key: str = Path(..., pattern="^[0-9a-zA-Z-]{0,128}$")):
    if key in store:
        return Response(content=store[key], media_type="text/plain", status_code=200)
    return Response(status_code=404, content="")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)