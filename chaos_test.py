import asyncio
import grpc
import sys
import os
import random
import docker

# Ensure generated protobufs are on the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
from generated import cache_pb2, cache_pb2_grpc
from hash_ring import ConsistentHashRing

# --- Configuration ---
NODE_ADDRESSES = ["localhost:50051", "localhost:50052", "localhost:50053"]
CONTAINER_NAMES = {"localhost:50051": "node1", "localhost:50052": "node2", "localhost:50053": "node3"}
KEY = "chaos_key"

async def run_chaos_test():
    """Simulates a node failure and verifies data is still accessible."""
    print("\n==================================================")
    print("CHAOS TEST: Proving Fault Tolerance")
    print("==================================================\n")

    hash_ring = ConsistentHashRing(list(CONTAINER_NAMES.values()), replicas=256)
    
    # --- CRITICAL FIX: Explicitly connect to the Docker socket ---
    try:
        docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    except Exception as e:
        print(f"❌ FAILED: Could not connect to Docker daemon. Is Docker running? Error: {e}")
        sys.exit(1)


    # --- Step 1: Write data to the cluster ---
    replicas = hash_ring.get_nodes(KEY, replication_factor=3)
    coordinator_address = random.choice(NODE_ADDRESSES)
    print(f"[Step 1] Connecting to coordinator '{coordinator_address}' to SET key='{KEY}'")
    print(f" -> This key should be replicated to: {replicas}")

    try:
        async with grpc.aio.insecure_channel(coordinator_address) as channel:
            stub = cache_pb2_grpc.CacheServiceStub(channel)
            await stub.Set(cache_pb2.SetRequest(key=KEY, value=b"data_survives"))
        print(" -> SET successful. Data is replicated.\n")
    except grpc.aio.AioRpcError as e:
        print(f"❌ FAILED: Could not set initial key. {e.details()}")
        sys.exit(1)

    # --- Step 2: Kill a random replica node ---
    node_to_kill_container = random.choice(replicas)
    print(f"[Step 2] Choosing a random replica to kill: '{node_to_kill_container}'")
    try:
        container_to_kill = docker_client.containers.get(node_to_kill_container)
        container_to_kill.kill()
        print(f" -> Successfully killed container '{node_to_kill_container}'. Waiting 5 seconds...\n")
        await asyncio.sleep(5)
    except docker.errors.NotFound:
        print(f"❌ FAILED: Could not find container '{node_to_kill_container}' to kill.")
        sys.exit(1)

    # --- Step 3: Read data from a surviving node ---
    survivors = [addr for addr in NODE_ADDRESSES if CONTAINER_NAMES[addr] != node_to_kill_container]
    if not survivors:
        print("❌ FAILED: No surviving nodes to connect to.")
        sys.exit(1)

    survivor_address = random.choice(survivors)
    print(f"[Step 3] Connecting to a surviving node '{survivor_address}' to GET key='{KEY}'")
    
    success = False
    try:
        async with grpc.aio.insecure_channel(survivor_address) as channel:
            stub = cache_pb2_grpc.CacheServiceStub(channel)
            get_response = await stub.Get(cache_pb2.GetRequest(key=KEY))
        
        if get_response.found and get_response.value == b"data_survives":
            print("✅ SUCCESS: Data was retrieved from a surviving replica!")
            success = True
        else:
            print("❌ FAILED: Data was lost or incorrect after node failure.")
    except grpc.aio.AioRpcError as e:
        print(f"❌ FAILED: Could not connect to surviving node. {e.details()}")
    
    finally:
        # --- Cleanup ---
        print("\n[Cleanup] Restarting the killed node...")
        try:
            container_to_kill.start()
            print("Cluster restored.")
        except Exception as e:
            print(f"Warning: could not restart killed node. {e}")
        
        if not success:
            sys.exit(1)

if __name__ == '__main__':
    asyncio.run(run_chaos_test()) 