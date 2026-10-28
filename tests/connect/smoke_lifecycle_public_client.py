"""Exercise Spark Connect replay and cleanup over a real gRPC connection."""

import time
import uuid

import grpc
from pyspark.sql.connect.proto import base_pb2, base_pb2_grpc


def main() -> None:
    address = "127.0.0.1:15002"
    session_id = str(uuid.uuid4())
    operation_id = str(uuid.uuid4())
    request = base_pb2.ExecutePlanRequest(
        session_id=session_id,
        operation_id=operation_id,
        user_context=base_pb2.UserContext(user_id="slice-five-network-smoke"),
    )
    request.plan.root.range.start = 0
    request.plan.root.range.end = 20_000
    request.plan.root.range.step = 1
    request.plan.root.range.num_partitions = 1
    request.request_options.add().reattach_options.reattachable = True

    first_channel = grpc.insecure_channel(address)
    grpc.channel_ready_future(first_channel).result(timeout=10)
    first_stub = base_pb2_grpc.SparkConnectServiceStub(first_channel)
    stream = first_stub.ExecutePlan(request, timeout=20)
    first = next(stream)
    assert first.HasField("arrow_batch")
    assert first.arrow_batch.start_offset == 0
    first_response_id = first.response_id

    # Closing the transport after the first response simulates losing the live stream.
    first_channel.close()
    time.sleep(0.05)

    resumed_channel = grpc.insecure_channel(address)
    grpc.channel_ready_future(resumed_channel).result(timeout=10)
    resumed_stub = base_pb2_grpc.SparkConnectServiceStub(resumed_channel)
    resumed_stub.ReleaseExecute(
        base_pb2.ReleaseExecuteRequest(
            session_id=session_id,
            user_context=base_pb2.UserContext(user_id="slice-five-network-smoke"),
            operation_id=operation_id,
            release_until=base_pb2.ReleaseExecuteRequest.ReleaseUntil(
                response_id=first_response_id
            ),
        ),
        timeout=10,
    )
    resumed = list(
        resumed_stub.ReattachExecute(
            base_pb2.ReattachExecuteRequest(
                session_id=session_id,
                user_context=base_pb2.UserContext(user_id="slice-five-network-smoke"),
                operation_id=operation_id,
                last_response_id=first_response_id,
            ),
            timeout=10,
        )
    )
    assert len(resumed) == 2, resumed
    assert resumed[0].HasField("arrow_batch")
    assert resumed[0].arrow_batch.start_offset == 16_384
    assert resumed[1].HasField("result_complete")
    assert all(response.operation_id == operation_id for response in resumed)

    resumed_stub.ReleaseExecute(
        base_pb2.ReleaseExecuteRequest(
            session_id=session_id,
            user_context=base_pb2.UserContext(user_id="slice-five-network-smoke"),
            operation_id=operation_id,
            release_all=base_pb2.ReleaseExecuteRequest.ReleaseAll(),
        ),
        timeout=10,
    )
    resumed_channel.close()
    print("network disconnect, prefix release, replay, completion, and release passed")


if __name__ == "__main__":
    main()
