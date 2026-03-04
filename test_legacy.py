import asyncio
import grpc
import sys
import os

# Ensure generated protobufs are on the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
from generated import cache_pb2, cache_pb2_grpc

async def run_with_retry(retries=5, delay=2):
    """Attempts to connect to the server with a retry policy."""
    address = 'localhost:50051'
    for i in range(retries):
        try:
            async with grpc.aio.insecure_channel(address) as channel:
                stub = cache_pb2_grpc.CacheServiceStub(channel)
                
                print(f"Attempt {i+1}: Querying for 'user:1001'...")
                # We use a short timeout to fail fast and retry
                response = await stub.Get(cache_pb2.GetRequest(key="user:1001"), timeout=5)
                
                if response.found and response.value.decode() == "Dr. Heisenberg":
                    print("✅ SUCCESS: Fetched data from Legacy API!")
                    return True
                else:
                    print("❌ FAILED: Unexpected response from server.")
                    return False
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            print(f"⚠️ Connection failed (Attempt {i+1}/{retries}). Retrying in {delay}s...")
            await asyncio.sleep(delay)
    
    print("❌ FATAL: Could not connect to server after multiple retries.")
    return False

if __name__ == '__main__':
    success = asyncio.run(run_with_retry())
    if not success:
        sys.exit(1)