from fastapi import FastAPI, HTTPException
import uvicorn

app = FastAPI()

# 1. Hardcoded data representing the "Old SQL Database"
LEGACY_DATA_STORE = {
    "user:1001": "Dr. Heisenberg",
    "user:1002": "Jesse Pinkman",
    "config:legacy_mode": "TRUE"
}

@app.get("/legacy/data/{key}")
async def get_legacy_data(key: str):
    print(f"[Legacy API] Received query for: {key}")
    if key in LEGACY_DATA_STORE:
        return {"key": key, "value": LEGACY_DATA_STORE[key]}
    else:
        raise HTTPException(status_code=404, detail="Key not found in legacy system")

if __name__ == "__main__":
    # We run on port 8001 to avoid conflicting with the cache nodes
    uvicorn.run(app, host="0.0.0.0", port=8001)
