import asyncio
import grpc
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
from generated import cache_pb2, cache_pb2_grpc

async def run():
    # Connect to localhost:50051 (mapped to node1)
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = cache_pb2_grpc.CacheServiceStub(channel)
        
        # Query a key that ONLY exists in the Legacy API
        print("Querying for 'user:1001'...")
        response = await stub.Get(cache_pb2.GetRequest(key="user:1001"))
        
        if response.found and response.value.decode() == "Dr. Heisenberg":
            print("✅ SUCCESS: Fetched data from Legacy API!")
        else:
            print("❌ FAILED: Could not fetch from Legacy API.")

if __name__ == '__main__':
    asyncio.run(run())
