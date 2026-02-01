import sys
import os
import asyncio
import grpc
import httpx    # Sprint 1: For Legacy API
import asyncpg  # Sprint 2: For PostgreSQL

from aiohttp import web
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
from generated import cache_pb2
from generated import cache_pb2_grpc

from hash_ring import ConsistentHashRing

REPLICATION_FACTOR = 3

# --- Telemetry Setup (Disabled for Local Docker Test) ---
def setup_telemetry(service_name: str):
    """Configures and initializes OpenTelemetry tracing."""
    resource = Resource(attributes={"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint="otel-collector-service:4317", insecure=True)
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    print(f"[{service_name}] OpenTelemetry tracing initialized.")

# --- Health Checks ---
async def health_check(request):
    return web.Response(text="OK")

async def readiness_check(request):
    return web.Response(text="OK")

async def start_health_server(port=8080):
    app = web.Application()
    app.router.add_get('/healthz', health_check)
    app.router.add_get('/readyz', readiness_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    print(f"Starting health check server on port {port}")
    await site.start()

# --- Core Service Logic ---
class CacheServiceServicer(cache_pb2_grpc.CacheServiceServicer):
    def __init__(self, my_address: str, all_nodes: list[str]):
        self.data = {}
        self.my_address = my_address
        self.ring = ConsistentHashRing(all_nodes)
        self.peer_stubs = {}
        self.db_pool = None  # Holds the PostgreSQL connection pool
        print(f"[{self.my_address}] Servicer initialized.")
        print(f"[{self.my_address}] Hash ring configured with nodes: {all_nodes}")

    async def _get_peer_stub(self, peer_address: str):
        if peer_address not in self.peer_stubs:
            channel = grpc.aio.insecure_channel(peer_address)
            self.peer_stubs[peer_address] = cache_pb2_grpc.CacheServiceStub(channel)
        return self.peer_stubs[peer_address]

    # --- FEATURE 2: Write-Through to PostgreSQL ---
    async def Set(self, request: cache_pb2.SetRequest, context) -> cache_pb2.SetResponse:
        key = request.key

        # Check metadata to prevent infinite replication loops
        is_replication_request = any(k == 'is-replication' for k, v in context.invocation_metadata())

        # If I am just a replica, I only write to RAM.
        if is_replication_request:
            self.data[key] = request.value
            return cache_pb2.SetResponse(success=True)

        # If I am the Coordinator (Client called me directly):
        target_nodes = self.ring.get_nodes(key, REPLICATION_FACTOR)
        tasks = []
        replication_metadata = [('is-replication', 'true')]

        # 1. Persistence Task: Write to PostgreSQL asynchronously
        if self.db_pool:
            db_query = """
                INSERT INTO kv_store (key, value) VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET value = $2
            """
            # We schedule this task to run in parallel with network replication
            tasks.append(self.db_pool.execute(db_query, key, request.value))

        # 2. Replication Tasks: Send to other nodes
        for node in target_nodes:
            if node == self.my_address:
                self.data[key] = request.value
            else:
                peer_stub = await self._get_peer_stub(node)
                task = peer_stub.Set(request, metadata=replication_metadata)
                tasks.append(task)

        # 3. Wait for everything to finish (Parallel Execution)
        if tasks:
            await asyncio.gather(*tasks)

        return cache_pb2.SetResponse(success=True)

    # --- FEATURE 1: Read-Through Legacy API ---
    async def Get(self, request: cache_pb2.GetRequest, context) -> cache_pb2.GetResponse:
        key = request.key
        
        # 1. Fast Path: Check Local In-Memory Cache
        value = self.data.get(key)
        if value is not None:
            return cache_pb2.GetResponse(value=value, found=True)
        
        # 2. Slow Path: Cache Miss -> Query Legacy API
        # print(f"[{self.my_address}] Key '{key}' MISS. Querying Legacy API...")
        try:
            async with httpx.AsyncClient() as client:
                # 'legacy_api' is the docker service name, port 8001
                resp = await client.get(f"http://legacy_api:8001/legacy/data/{key}", timeout=1.0)
            
            if resp.status_code == 200:
                legacy_val = resp.json()['value']
                print(f"[{self.my_address}] HIT from Legacy API: {legacy_val}")
                return cache_pb2.GetResponse(value=legacy_val.encode('utf-8'), found=True)
            # else:
            #     print(f"[{self.my_address}] MISS from Legacy API.")

        except Exception as e:
            print(f"[{self.my_address}] Legacy API Error: {e}")

        return cache_pb2.GetResponse(found=False)


# --- Server Startup ---
async def serve(address: str, all_nodes: list[str]):
    # Instrument gRPC for telemetry
    grpc_server_instrumentor = GrpcInstrumentorServer()
    grpc_server_instrumentor.instrument()

    port = address.split(':')[-1]
    bind_address = f"0.0.0.0:{port}"

    # Initialize Servicer
    servicer = CacheServiceServicer(address, all_nodes)

    # --- Setup PostgreSQL Connection ---
    try:
        servicer.db_pool = await asyncpg.create_pool(
            dsn='postgres://admin:password123@postgres_db:5432/cache_db',
            min_size=1,
            max_size=10
        )
        print(f"[{address}] Connected to PostgreSQL Persistence Layer.")
    except Exception as e:
        print(f"[{address}] WARNING: Database connection failed. Running in-memory only. Error: {e}")
    # -----------------------------------

    server = grpc.aio.server()
    cache_pb2_grpc.add_CacheServiceServicer_to_server(servicer, server)
    server.add_insecure_port(bind_address)

    print(f"Starting gRPC server with identity '{address}' on bind address '{bind_address}'")
    await server.start()
    asyncio.create_task(start_health_server())
    await server.wait_for_termination()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python server.py <my_address_in_cluster>")
        sys.exit(1)

    my_address = sys.argv[1]

    # --- Hybrid Environment Detection ---
    if '.' not in my_address:
        print(f"[{my_address}] Detected Docker Compose environment.")
        # Hardcoded peers for local dev
        all_node_addresses = ["node1:50051", "node2:50052", "node3:50053"]
    else:
        # Kubernetes StatefulSet logic
        my_hostname = my_address.split('.')[0]
        service_name = my_address.split('.')[1].split(':')[0]
        all_node_addresses = []
        hostname_base = my_hostname.rsplit('-', 1)[0]
        for i in range(REPLICATION_FACTOR):
            peer_hostname = f"{hostname_base}-{i}"
            peer_address = f"{peer_hostname}.{service_name}:50051"
            all_node_addresses.append(peer_address)

    print(f"My address: {my_address}")
    print(f"All nodes in cluster: {all_node_addresses}")

    # Telemetry is disabled for local Docker Compose to prevent crashes
    # setup_telemetry("cache-service")
    print("Telemetry disabled for local test.")

    asyncio.run(serve(my_address, all_node_addresses))
