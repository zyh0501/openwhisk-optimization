import os
import uuid
from time import time

import boto3
import cv2

TMP_DIR = "/tmp/"

# Keys that A should pass through for B.
_PASSTHROUGH_KEYS = (
    "endpoint_url",
    "aws_access_key_id",
    "aws_secret_access_key",
    "output_bucket",
    "metadata",
)


def _process_stage_a(object_key, video_path):
    file_name = os.path.splitext(os.path.basename(object_key))[0]
    result_file_path = os.path.join(TMP_DIR, file_name + "-stage-a.avi")

    video = cv2.VideoCapture(video_path)
    if not video.isOpened():
        raise RuntimeError("stage_a_open_video_failed")
    fourcc = cv2.VideoWriter_fourcc(*"XVID")
    out = None
    frame_count = 0

    start = time()
    while video.isOpened():
        ret, frame = video.read()
        if not ret:
            break

        gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gaussian = cv2.GaussianBlur(gray_frame, (5, 5), 0)
        denoised_gray = cv2.medianBlur(gaussian, 5)
        # Write 3-channel frames to improve codec compatibility.
        denoised_frame = cv2.cvtColor(denoised_gray, cv2.COLOR_GRAY2BGR)

        if out is None:
            frame_height, frame_width = denoised_frame.shape[:2]
            out = cv2.VideoWriter(result_file_path, fourcc, 20.0, (frame_width, frame_height))

        out.write(denoised_frame)
        frame_count += 1

    latency = time() - start
    video.release()
    if out is not None:
        out.release()
    if frame_count == 0 or (not os.path.isfile(result_file_path)):
        raise RuntimeError("stage_a_no_output_video_generated")
    return latency, result_file_path


def main(event):
    event = dict(event or {})
    latencies = {}
    timestamps = {"starting_time": time()}

    input_bucket = event["input_bucket"]
    object_key = event["object_key"]
    output_bucket = event["output_bucket"]
    endpoint_url = event["endpoint_url"]
    aws_access_key_id = event["aws_access_key_id"]
    aws_secret_access_key = event["aws_secret_access_key"]
    metadata = event.get("metadata", {})

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    safe_object_name = os.path.basename(object_key)
    download_path = os.path.join(TMP_DIR, "{}-{}".format(uuid.uuid4().hex, safe_object_name))

    t0 = time()
    s3_client.download_file(input_bucket, object_key, download_path)
    latencies["download_data"] = time() - t0

    stage_a_latency, stage_a_path = _process_stage_a(object_key, download_path)
    latencies["function_execution_A"] = stage_a_latency

    inter_key = "video-processing-intermediate/{}-stage-a.avi".format(uuid.uuid4().hex)
    t1 = time()
    s3_client.upload_file(stage_a_path, output_bucket, inter_key)
    latencies["upload_intermediate"] = time() - t1

    timestamps["finishing_time"] = time()
    result = {
        "bucket": output_bucket,
        "object_key": inter_key,
        "latencies": latencies,
        "timestamps": timestamps,
        "metadata": metadata,
    }
    for k in _PASSTHROUGH_KEYS:
        if k in event:
            result[k] = event[k]
    return result
