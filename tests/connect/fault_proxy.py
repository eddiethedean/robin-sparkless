"""Raw Spark Connect gRPC forwarding proxy with one mid-stream UNAVAILABLE fault."""

from __future__ import annotations

import argparse
import threading
from concurrent.futures import ThreadPoolExecutor

import grpc
from pyspark.sql.connect.proto import base_pb2


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True)
    parser.add_argument("--upstream", required=True)
    args = parser.parse_args()

    upstream_channel = grpc.insecure_channel(args.upstream)
    fault_injected = threading.Event()
    handlers = {}
    for method in base_pb2.DESCRIPTOR.services_by_name["SparkConnectService"].methods:
        path = f"/spark.connect.SparkConnectService/{method.name}"
        if method.client_streaming and method.server_streaming:
            upstream_call = upstream_channel.stream_stream(
                path, request_serializer=bytes, response_deserializer=bytes
            )
            handler = grpc.stream_stream_rpc_method_handler(
                lambda requests, context, call=upstream_call: call(requests),
                request_deserializer=None,
                response_serializer=None,
            )
        elif method.client_streaming:
            upstream_call = upstream_channel.stream_unary(
                path, request_serializer=bytes, response_deserializer=bytes
            )
            handler = grpc.stream_unary_rpc_method_handler(
                lambda requests, context, call=upstream_call: call(requests),
                request_deserializer=None,
                response_serializer=None,
            )
        elif method.server_streaming:
            upstream_call = upstream_channel.unary_stream(
                path, request_serializer=bytes, response_deserializer=bytes
            )

            def forward_stream(request, context, call=upstream_call, name=method.name):
                call_result = call(request)
                for index, response in enumerate(call_result):
                    yield response
                    if name == "ExecutePlan" and index == 0 and not fault_injected.is_set():
                        fault_injected.set()
                        print(
                            "fault_proxy injected UNAVAILABLE after first ExecutePlan response",
                            flush=True,
                        )
                        call_result.cancel()
                        context.abort(
                            grpc.StatusCode.UNAVAILABLE,
                            "injected mid-stream disconnect for reattachment coverage",
                        )

            handler = grpc.unary_stream_rpc_method_handler(
                forward_stream,
                request_deserializer=None,
                response_serializer=None,
            )
        else:
            upstream_call = upstream_channel.unary_unary(
                path, request_serializer=bytes, response_deserializer=bytes
            )
            handler = grpc.unary_unary_rpc_method_handler(
                lambda request, context, call=upstream_call: call(request),
                request_deserializer=None,
                response_serializer=None,
            )
        handlers[method.name] = handler

    server = grpc.server(ThreadPoolExecutor(max_workers=16))
    server.add_generic_rpc_handlers(
        (grpc.method_handlers_generic_handler("spark.connect.SparkConnectService", handlers),)
    )
    if server.add_insecure_port(args.listen) == 0:
        raise RuntimeError(f"could not bind fault proxy to {args.listen}")
    server.start()
    print(f"fault_proxy ready address={args.listen}", flush=True)
    server.wait_for_termination()
    upstream_channel.close()


if __name__ == "__main__":
    main()
