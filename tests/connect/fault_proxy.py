"""Raw Spark Connect gRPC forwarding proxy with one mid-stream UNAVAILABLE fault."""

from __future__ import annotations

import argparse
import signal
import threading
from concurrent.futures import ThreadPoolExecutor

import grpc
from pyspark.sql.connect.proto import base_pb2


def forwarded_metadata(context):
    return tuple(context.invocation_metadata() or ())


def abort_upstream(error, context):
    code = error.code() or grpc.StatusCode.INTERNAL
    details = error.details() or "upstream Spark Connect RPC failed"
    try:
        trailers = error.trailing_metadata() or ()
    except grpc.RpcError:
        trailers = ()
    if trailers:
        context.set_trailing_metadata(trailers)
    context.abort(code, details)


def forward_unary(call, request, context):
    try:
        response, call_result = call.with_call(
            request, metadata=forwarded_metadata(context)
        )
    except grpc.RpcError as error:
        abort_upstream(error, context)
    context.send_initial_metadata(call_result.initial_metadata() or ())
    context.set_trailing_metadata(call_result.trailing_metadata() or ())
    return response


def forward_client_stream(call, requests, context):
    try:
        response, call_result = call.with_call(
            requests, metadata=forwarded_metadata(context)
        )
    except grpc.RpcError as error:
        abort_upstream(error, context)
    context.send_initial_metadata(call_result.initial_metadata() or ())
    context.set_trailing_metadata(call_result.trailing_metadata() or ())
    return response


def forward_stream(call_result, context):
    try:
        context.send_initial_metadata(call_result.initial_metadata() or ())
        yield from call_result
    except grpc.RpcError as error:
        abort_upstream(error, context)
    context.set_trailing_metadata(call_result.trailing_metadata() or ())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--upstream-tls", action="store_true")
    parser.add_argument("--upstream-local-auth", action="store_true")
    parser.add_argument(
        "--local-auth",
        action="store_true",
        help="serve localhost with gRPC local credentials so token URLs can be used",
    )
    args = parser.parse_args()

    if args.upstream_tls and args.upstream_local_auth:
        parser.error("--upstream-tls and --upstream-local-auth cannot be combined")
    if args.upstream_tls:
        upstream_channel = grpc.secure_channel(
            args.upstream, grpc.ssl_channel_credentials()
        )
    elif args.upstream_local_auth:
        upstream_channel = grpc.secure_channel(
            args.upstream, grpc.local_channel_credentials()
        )
    else:
        upstream_channel = grpc.insecure_channel(args.upstream)
    fault_injected = threading.Event()
    handlers = {}
    for method in base_pb2.DESCRIPTOR.services_by_name["SparkConnectService"].methods:
        path = f"/spark.connect.SparkConnectService/{method.name}"
        if method.client_streaming and method.server_streaming:
            upstream_call = upstream_channel.stream_stream(
                path, request_serializer=bytes, response_deserializer=bytes
            )

            def forward_bidi(requests, context, call=upstream_call):
                return forward_stream(
                    call(requests, metadata=forwarded_metadata(context)), context
                )

            handler = grpc.stream_stream_rpc_method_handler(
                forward_bidi,
                request_deserializer=None,
                response_serializer=None,
            )
        elif method.client_streaming:
            upstream_call = upstream_channel.stream_unary(
                path, request_serializer=bytes, response_deserializer=bytes
            )
            handler = grpc.stream_unary_rpc_method_handler(
                lambda requests, context, call=upstream_call: forward_client_stream(
                    call, requests, context
                ),
                request_deserializer=None,
                response_serializer=None,
            )
        elif method.server_streaming:
            upstream_call = upstream_channel.unary_stream(
                path, request_serializer=bytes, response_deserializer=bytes
            )

            def forward_server_stream(
                request, context, call=upstream_call, name=method.name
            ):
                call_result = call(request, metadata=forwarded_metadata(context))
                try:
                    context.send_initial_metadata(call_result.initial_metadata() or ())
                    for index, response in enumerate(call_result):
                        yield response
                        if (
                            name == "ExecutePlan"
                            and index == 0
                            and not fault_injected.is_set()
                        ):
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
                except grpc.RpcError as error:
                    abort_upstream(error, context)
                context.set_trailing_metadata(call_result.trailing_metadata() or ())

            handler = grpc.unary_stream_rpc_method_handler(
                forward_server_stream,
                request_deserializer=None,
                response_serializer=None,
            )
        else:
            upstream_call = upstream_channel.unary_unary(
                path, request_serializer=bytes, response_deserializer=bytes
            )
            handler = grpc.unary_unary_rpc_method_handler(
                lambda request, context, call=upstream_call: forward_unary(
                    call, request, context
                ),
                request_deserializer=None,
                response_serializer=None,
            )
        handlers[method.name] = handler

    server = grpc.server(ThreadPoolExecutor(max_workers=16))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "spark.connect.SparkConnectService", handlers
            ),
        )
    )
    if args.local_auth:
        credentials = grpc.local_server_credentials(grpc.LocalConnectionType.LOCAL_TCP)
        bound_port = server.add_secure_port(args.listen, credentials)
    else:
        bound_port = server.add_insecure_port(args.listen)
    if bound_port == 0:
        raise RuntimeError(f"could not bind fault proxy to {args.listen}")
    server.start()
    print(f"fault_proxy ready address={args.listen}", flush=True)

    def shutdown(_signal_number, _frame):
        server.stop(grace=2).wait(timeout=5)
        upstream_channel.close()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    server.wait_for_termination()
    upstream_channel.close()


if __name__ == "__main__":
    main()
