import sys
import os
import asyncio
import grpc
import random
import subprocess
import time

sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
from generated import cache_pb2
from generated import cache_pb2_grpc
from hash_ring import ConsistentHashRing

# Addresses for connecting from the HOST machine ( terminal)
HOST_NODE_ADDRESSES = ["localhost:50051", "localhost:50052", "localhost:50053"]
# Addresses as they are known INSIDE the Docker network
DOCKER_NODE_ADDRESSES = ["node1:50051", "node2:50052", "node3:50053"]
REPLICATION_FACTOR = 3

async def run_chaos_test():
    print("="*50)
    print("    CHAOS TEST: Proving Fault Tolerance")
    print("="*50)

    # The hash ring must be initialized with the names the SERVERS use.
    ring = ConsistentHashRing(DOCKER_NODE_ADDRESSES)

    key = "chaos_key"
    value = "this_data_must_survive".encode('utf-8')
    
    # --- Step 1: Connecting to a random node and SET the value ---
    coordinator_address = random.choice(HOST_NODE_ADDRESSES)
    print(f"\n[Step 1] Connecting to coordinator '{coordinator_address}' to SET key='{key}'")
    
    # Using the ring to find the internal Docker names of the target nodes.
    target_nodes_docker = ring.get_nodes(key, REPLICATION_FACTOR)
    print(f" -> This key should be replicated to: {target_nodes_docker}")
    
    async with grpc.aio.insecure_channel(coordinator_address) as channel:
        stub = cache_pb2_grpc.CacheServiceStub(channel)
        set_response = await stub.Set(cache_pb2.SetRequest(key=key, value=value))
        assert set_response.success
    print(" -> SET successful. Data is replicated.")

    # --- Step 2: Choosing a victim node to kill ---
    victim_node_docker_name = random.choice(target_nodes_docker).split(':')[0]
    print(f"\n[Step 2] Choosing a random replica to kill: '{victim_node_docker_name}'")
    
    subprocess.run(["docker", "kill", victim_node_docker_name], check=True, capture_output=True)
    print(f" -> Successfully killed container '{victim_node_docker_name}'. Waiting 5 seconds...")
    time.sleep(5)

    # --- Step 3: Connecting to a SURVIVING node and GET the value ---
    surviving_nodes_host = [addr for addr in HOST_NODE_ADDRESSES if victim_node_docker_name not in addr]
    accessor_address = random.choice(surviving_nodes_host)
    print(f"\n[Step 3] Connecting to a surviving node '{accessor_address}' to GET key='{key}'")

    try:
        async with grpc.aio.insecure_channel(accessor_address) as channel:
            stub = cache_pb2_grpc.CacheServiceStub(channel)
            get_response = await stub.Get(cache_pb2.GetRequest(key=key))

            if get_response.found and get_response.value == value:
                print("\n" + "="*50)
                print("  ✅ CHAOS TEST PASSED ✅")
                print(f"  Successfully retrieved key='{key}' even after its owner '{victim_node_docker_name}' was killed.")
                print("="*50)
            else:
                # To Add more debug info
                if not get_response.found:
                    raise AssertionError("Failed to retrieve value from a surviving node: KEY NOT FOUND.")
                else:
                    raise AssertionError(f"Failed to retrieve value: VALUE MISMATCH. Got '{get_response.value.decode()}'")
    finally:
        print("\n[Cleanup] Restarting the killed node...")
        subprocess.run(["docker", "start", victim_node_docker_name], check=True, capture_output=True)
        print("Cluster restored.")


if __name__ == '__main__':
    asyncio.run(run_chaos_test())