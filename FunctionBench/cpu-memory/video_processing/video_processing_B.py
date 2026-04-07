import os
import uuid
from time import time

import boto3
import cv2

TMP_DIR = "/tmp/"


def _process_stage_b(object_key, video_path):
    file_name = os.path.splitext(os.path.basename(object_key))[0]
    result_file_path = os.path.join(TMP_DIR, file_name + "-stage-b.avi")
    tmp_file_path = os.path.join(TMP_DIR, "tmp.jpg")

    video = cv2.VideoCapture(video_path)
    if not video.isOpened():
        raise RuntimeError("stage_b_open_video_failed")
    fourcc = cv2.VideoWriter_fourcc(*"XVID")
    out = None
    frame_count = 0

    start = time()
    while video.isOpened():
        ret, frame = video.read()
        if not ret:
            break

        cv2.imwrite(tmp_file_path, frame)
        jpg_frame = cv2.imread(tmp_file_path)

        if out is None:
            frame_height, frame_width = jpg_frame.shape[:2]
            out = cv2.VideoWriter(result_file_path, fourcc, 20.0, (frame_width, frame_height))

        out.write(jpg_frame)
        frame_count += 1

    latency = time() - start
    video.release()
    if out is not None:
        out.release()
    if frame_count == 0 or (not os.path.isfile(result_file_path)):
        raise RuntimeError("stage_b_no_output_video_generated")
    return latency, result_file_path


def main(event):
    event = dict(event or {})
    latencies = {}
    timestamps = {"starting_time": time()}

    # In sequence, A returns bucket/object_key of intermediate artifact.
    input_bucket = event.get("bucket") or event.get("input_bucket")
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
    latencies["download_intermediate"] = time() - t0

    stage_b_latency, upload_path = _process_stage_b(object_key, download_path)
    latencies["function_execution_B"] = stage_b_latency

    t1 = time()
    s3_client.upload_file(upload_path, output_bucket, os.path.basename(upload_path))
    latencies["upload_data"] = time() - t1

    timestamps["finishing_time"] = time()
    return {"latencies": latencies, "timestamps": timestamps, "metadata": metadata}
