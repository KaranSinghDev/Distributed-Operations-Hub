import sys
import os
import asyncio
import grpc
import random
import time
import uuid
import numpy as np

# --- OpenTelemetry Imports ---
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient

sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))
from generated import cache_pb2
from generated import cache_pb2_grpc


# For local testing, we connect through the gRPC port-forward tunnel.
NODE_ADDRESSES = ["localhost:50051"]
TOTAL_OPERATIONS = 100 
CONCURRENCY = 5

# --- OpenTelemetry Setup ---
def setup_telemetry(service_name: str):
    """Initializes OpenTelemetry for the client."""
    resource = Resource(attributes={"service.name": service_name})
    provider = TracerProvider(resource=resource)
    # The benchmark runs locally, so it connects to the Collector via its own port-forward
    exporter = OTLPSpanExporter(endpoint="localhost:4317", insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    
    # Auto-instrument all outgoing gRPC client calls
    GrpcInstrumentorClient().instrument()
    print(f"[{service_name}] OpenTelemetry tracing initialized for client.")

# --- Helper Functions---
def print_header(title):
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

def print_stats(label, latencies_ms, duration_s, total_ops, errors):
    if not latencies_ms:
        print(f"\n--- {label} Results ---")
        print("  No successful operations recorded.")
        return
    p99 = np.percentile(latencies_ms, 99)
    throughput = total_ops / duration_s
    print(f"\n--- {label} Results ---")
    print(f"Throughput: {throughput:,.0f} ops/sec, p99 Latency: {p99:.4f} ms, Errors: {errors}")

# --- Worker Logic ---
async def worker(worker_id, ops_queue, results_list, error_count):
    node_address = random.choice(NODE_ADDRESSES)
    latencies = []
    errors = 0
    try:
        async with grpc.aio.insecure_channel(node_address) as channel:
            stub = cache_pb2_grpc.CacheServiceStub(channel)
            while not ops_queue.empty():
                op_type, key, value = await ops_queue.get()
                start_time = time.perf_counter()
                try:
                    if op_type == 'SET':
                        await stub.Set(cache_pb2.SetRequest(key=key, value=value))
                    elif op_type == 'GET':
                        await stub.Get(cache_pb2.GetRequest(key=key))
                    end_time = time.perf_counter()
                    latencies.append((end_time - start_time) * 1000)
                except grpc.aio.AioRpcError:
                    errors += 1
                ops_queue.task_done()
    except Exception:
        errors += ops_queue.qsize()
    results_list.extend(latencies)
    error_count[worker_id] = errors

# --- Main Benchmark Orchestrator ---
async def run_benchmark():
    print_header("Concurrent Key-Value Store Benchmark")
    print(f"Configuration: {CONCURRENCY} concurrent clients, {TOTAL_OPERATIONS:,} total operations")

    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("benchmark-set-operation") as span:
        ops_queue = asyncio.Queue()
        keys_to_get = []
        for i in range(TOTAL_OPERATIONS):
            key = uuid.uuid4().hex
            keys_to_get.append(key)
            value = b'benchmark_value'
            await ops_queue.put(('SET', key, value))

        set_latencies = []
        set_errors = [0] * CONCURRENCY
        start_time = time.perf_counter()
        worker_tasks = [
            asyncio.create_task(worker(i, ops_queue, set_latencies, set_errors))
            for i in range(CONCURRENCY)
        ]
        await asyncio.gather(*worker_tasks)
        end_time = time.perf_counter()
        print_stats("SET Benchmark", set_latencies, end_time - start_time, TOTAL_OPERATIONS, sum(set_errors))

    with tracer.start_as_current_span("benchmark-get-operation") as span:
        for key in keys_to_get:
            await ops_queue.put(('GET', key, None))
        
        get_latencies = []
        get_errors = [0] * CONCURRENCY
        start_time = time.perf_counter()
        worker_tasks = [
            asyncio.create_task(worker(i, ops_queue, get_latencies, get_errors))
            for i in range(CONCURRENCY)
        ]
        await asyncio.gather(*worker_tasks)
        end_time = time.perf_counter()
        print_stats("GET Benchmark (Hot Cache)", get_latencies, end_time - start_time, TOTAL_OPERATIONS, sum(get_errors))


if __name__ == '__main__':
    setup_telemetry("benchmark-client")
    
    print("Starting benchmark in 3 seconds... Ensure your cluster is up and port-forwards are running.")
    time.sleep(3)
    asyncio.run(run_benchmark())