import sys
import os
import asyncio
import grpc

sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
from generated import cache_pb2
from generated import cache_pb2_grpc

from hash_ring import ConsistentHashRing

NODE_ADDRESSES = ["node1:50051", "node2:50052", "node3:50053"]
REPLICATION_FACTOR = 3

async def run(connect_to_address: str):
    print(f"--- Client connecting to {connect_to_address} ---")
    ring = ConsistentHashRing(NODE_ADDRESSES)

    async with grpc.aio.insecure_channel(connect_to_address) as channel:
        stub = cache_pb2_grpc.CacheServiceStub(channel)

        key = "my_special_key"
        value = f"this_data_is_replicated".encode('utf-8')
        
        #  the list of nodes that should store this key.
        target_nodes = ring.get_nodes(key, REPLICATION_FACTOR)
        print(f"\nSetting key='{key}'. This key should be replicated to: {target_nodes}")
        
        set_response = await stub.Set(cache_pb2.SetRequest(key=key, value=value))
        assert set_response.success
        print(f" -> Set successful. Data is now on {len(target_nodes)} nodes.")

        print("\n" + "="*40)
        print("  Verification: Getting key from all replicas")
        print("="*40)

        # Verify that we can get the key from EACH of the target nodes.
        for node in target_nodes:
            print(f"Attempting to get key='{key}' directly from {node}...")
            async with grpc.aio.insecure_channel(node) as verify_channel:
                verify_stub = cache_pb2_grpc.CacheServiceStub(verify_channel)
                get_response = await verify_stub.Get(cache_pb2.GetRequest(key=key))
                if get_response.found:
                    print(f" -> SUCCESS! Found on {node}. Value: '{get_response.value.decode()}'")
                    assert get_response.value.decode() == value.decode()
                else:
                    print(f" -> ERROR: Key not found on replica {node}!")
                    assert False, "Key should have been found on all replicas"

if __name__ == '__main__':
    connect_to = NODE_ADDRESSES[0
    asyncio.run(run(connect_to))