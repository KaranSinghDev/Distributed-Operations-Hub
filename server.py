import sys
import os
import asyncio
import grpc

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

# --- NEW: OpenTelemetry Setup Function ---
def setup_telemetry(service_name: str):
    """Configures and initializes OpenTelemetry tracing."""
    # 1. Set a "Resource" to identify our service
    resource = Resource(attributes={
        "service.name": service_name
    })

    # 2. Set up the TracerProvider
    provider = TracerProvider(resource=resource)

    # 3. Configure the OTLP Exporter to send data to our Collector
    #    The endpoint must match the collector's service name in Kubernetes.
    exporter = OTLPSpanExporter(endpoint="otel-collector-service:4317", insecure=True)
    
    # 4. Use a BatchSpanProcessor to send traces in batches
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

    # 5. Set the global TracerProvider
    trace.set_tracer_provider(provider)
    print(f"[{service_name}] OpenTelemetry tracing initialized, exporting to otel-collector-service:4317")



async def health_check(request):
    """Liveness probe: returns OK if the Python process is running."""
    return web.Response(text="OK")

async def readiness_check(request):
    """Readiness probe: returns OK. Can be extended to check dependencies."""
    return web.Response(text="OK")

async def start_health_server(port=8080):
    """Starts the aiohttp server for health checks in the background."""
    app = web.Application()
    app.router.add_get('/healthz', health_check)
    app.router.add_get('/readyz', readiness_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    print(f"Starting health check server on port {port}")
    await site.start()


class CacheServiceServicer(cache_pb2_grpc.CacheServiceServicer):
    def __init__(self, my_address: str, all_nodes: list[str]):
        self.data = {}
        self.my_address = my_address
        self.ring = ConsistentHashRing(all_nodes)
        self.peer_stubs = {}
        print(f"[{self.my_address}] Servicer initialized.")
        print(f"[{self.my_address}] Hash ring configured with nodes: {all_nodes}")


    async def _get_peer_stub(self, peer_address: str):
        if peer_address not in self.peer_stubs:
            channel = grpc.aio.insecure_channel(peer_address)
            self.peer_stubs[peer_address] = cache_pb2_grpc.CacheServiceStub(channel)
        return self.peer_stubs[peer_address]

    async def Set(self, request: cache_pb2.SetRequest, context) -> cache_pb2.SetResponse:
            key = request.key
            
            is_replication_request = any(k == 'is-replication' for k, v in context.invocation_metadata())

            if is_replication_request:
                self.data[key] = request.value
                return cache_pb2.SetResponse(success=True)

            target_nodes = self.ring.get_nodes(key, REPLICATION_FACTOR)
            
            tasks = []
            replication_metadata = [('is-replication', 'true')]

            for node in target_nodes:
                if node == self.my_address:
                    self.data[key] = request.value
                else:
                    peer_stub = await self._get_peer_stub(node)
                    task = peer_stub.Set(request, metadata=replication_metadata)
                    tasks.append(task)
            
            if tasks:
                await asyncio.gather(*tasks)
                
            return cache_pb2.SetResponse(success=True)

    async def Get(self, request: cache_pb2.GetRequest, context) -> cache_pb2.GetResponse:
        key = request.key
        value = self.data.get(key)
        if value is not None:
            return cache_pb2.GetResponse(value=value, found=True)
        else:
            return cache_pb2.GetResponse(found=False)

# --- NEW: Import the server instrumentor ---
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer

async def serve(address: str, all_nodes: list[str]):
    grpc_server_instrumentor = GrpcInstrumentorServer()
    grpc_server_instrumentor.instrument()

    port = address.split(':')[-1]
    bind_address = f"0.0.0.0:{port}"

    server = grpc.aio.server()
    cache_pb2_grpc.add_CacheServiceServicer_to_server(CacheServiceServicer(address, all_nodes), server)
    
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

    setup_telemetry("cache-service")    
    asyncio.run(serve(my_address, all_node_addresses))