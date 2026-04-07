import json
import os
import subprocess
import sys
import tempfile
import uuid
from time import time

import boto3
import cv2

TMP_DIR = "/tmp/"
FILE_NAME_INDEX = 0
FILE_PATH_INDEX = 2


def video_processing(object_key, video_path):
    phase_latencies = {
        "video_open": 0.0,
        "writer_open": 0.0,
        "frame_read": 0.0,
        "to_grayscale": 0.0,
        "gaussian_blur": 0.0,
        "median_blur": 0.0,
        "imwrite_tmp": 0.0,
        "imread_tmp": 0.0,
        "write_output": 0.0,
        "resource_release": 0.0,
    }

    file_name = object_key.split(".")[FILE_NAME_INDEX]
    result_file_path = TMP_DIR + file_name + "-output.avi"
    tmp_file_path = TMP_DIR + "tmp.jpg"
    frame_count = 0

    open_start = time()
    video = cv2.VideoCapture(video_path)
    phase_latencies["video_open"] = time() - open_start

    fourcc = cv2.VideoWriter_fourcc(*"XVID")
    out = None

    processing_start = time()
    while video.isOpened():
        read_start = time()
        ret, frame = video.read()
        phase_latencies["frame_read"] += time() - read_start

        if not ret:
            break

        gray_start = time()
        gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        phase_latencies["to_grayscale"] += time() - gray_start

        gaussian_start = time()
        gaussian = cv2.GaussianBlur(gray_frame, (5, 5), 0)
        phase_latencies["gaussian_blur"] += time() - gaussian_start

        median_start = time()
        denoised_frame = cv2.medianBlur(gaussian, 5)
        phase_latencies["median_blur"] += time() - median_start

        imwrite_start = time()
        cv2.imwrite(tmp_file_path, denoised_frame)
        phase_latencies["imwrite_tmp"] += time() - imwrite_start

        imread_start = time()
        denoised_frame = cv2.imread(tmp_file_path)
        phase_latencies["imread_tmp"] += time() - imread_start

        if out is None:
            writer_start = time()
            frame_height, frame_width = denoised_frame.shape[:2]
            out = cv2.VideoWriter(
                result_file_path, fourcc, 20.0, (frame_width, frame_height)
            )
            phase_latencies["writer_open"] += time() - writer_start

        out_start = time()
        out.write(denoised_frame)
        phase_latencies["write_output"] += time() - out_start
        frame_count += 1

    processing_latency = time() - processing_start

    release_start = time()
    video.release()
    if out is not None:
        out.release()
    phase_latencies["resource_release"] = time() - release_start

    phase_latencies["function_execution"] = processing_latency
    phase_latencies["processed_frames"] = frame_count
    return processing_latency, result_file_path, phase_latencies


def _run_core(event):
    latencies = {}
    timestamps = {}

    timestamps["starting_time"] = time()
    input_bucket = event["input_bucket"]
    object_key = event["object_key"]
    output_bucket = event["output_bucket"]
    endpoint_url = event["endpoint_url"]
    aws_access_key_id = event["aws_access_key_id"]
    aws_secret_access_key = event["aws_secret_access_key"]
    metadata = event["metadata"]

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    download_path = TMP_DIR + "{}{}".format(uuid.uuid4(), object_key)

    start = time()
    s3_client.download_file(input_bucket, object_key, download_path)
    latencies["download_data"] = time() - start

    (
        video_processing_latency,
        upload_path,
        video_processing_phase_latencies,
    ) = video_processing(object_key, download_path)
    latencies["function_execution"] = video_processing_latency
    latencies["video_processing_phases"] = video_processing_phase_latencies

    start = time()
    s3_client.upload_file(upload_path, output_bucket, upload_path.split("/")[FILE_PATH_INDEX])
    latencies["upload_data"] = time() - start
    timestamps["finishing_time"] = time()

    return {"latencies": latencies, "timestamps": timestamps, "metadata": metadata}


def _profile_with_scalene(event):
    outfile = os.path.join(tempfile.gettempdir(), "scalene-video-processing.json")
    return_full_profile = bool(event.get("return_full_scalene_profile", False))
    log_scalene_view = bool(event.get("log_scalene_view", True))
    include_scalene_stderr = bool(event.get("include_scalene_stderr", False))

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as ef:
        json.dump(event, ef)
        event_path = ef.name

    core_result_path = None
    profile_script = None
    child_env = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_core_result.json", delete=False, encoding="utf-8"
        ) as crf:
            core_result_path = crf.name

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as sf:
            profile_script = sf.name
            script = f"""import json
import uuid
from time import time
import boto3
import cv2

TMP_DIR = "/tmp/"
FILE_NAME_INDEX = 0
FILE_PATH_INDEX = 2


def video_processing_profiled(object_key, video_path):
    phase_latencies = {{
        "video_open": 0.0,
        "writer_open": 0.0,
        "frame_read": 0.0,
        "to_grayscale": 0.0,
        "gaussian_blur": 0.0,
        "median_blur": 0.0,
        "imwrite_tmp": 0.0,
        "imread_tmp": 0.0,
        "write_output": 0.0,
        "resource_release": 0.0,
    }}

    file_name = object_key.split(".")[FILE_NAME_INDEX]
    result_file_path = TMP_DIR + file_name + "-output.avi"
    tmp_file_path = TMP_DIR + "tmp.jpg"
    frame_count = 0

    t0 = time()
    video = cv2.VideoCapture(video_path)
    phase_latencies["video_open"] = time() - t0
    fourcc = cv2.VideoWriter_fourcc(*"XVID")
    out = None

    processing_start = time()
    while video.isOpened():
        t1 = time()
        ret, frame = video.read()
        phase_latencies["frame_read"] += time() - t1
        if not ret:
            break

        t2 = time()
        gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        phase_latencies["to_grayscale"] += time() - t2

        t3 = time()
        gaussian = cv2.GaussianBlur(gray_frame, (5, 5), 0)
        phase_latencies["gaussian_blur"] += time() - t3

        t4 = time()
        denoised_frame = cv2.medianBlur(gaussian, 5)
        phase_latencies["median_blur"] += time() - t4

        t5 = time()
        cv2.imwrite(tmp_file_path, denoised_frame)
        phase_latencies["imwrite_tmp"] += time() - t5

        t6 = time()
        denoised_frame = cv2.imread(tmp_file_path)
        phase_latencies["imread_tmp"] += time() - t6

        if out is None:
            tw = time()
            frame_height, frame_width = denoised_frame.shape[:2]
            out = cv2.VideoWriter(result_file_path, fourcc, 20.0, (frame_width, frame_height))
            phase_latencies["writer_open"] += time() - tw

        t7 = time()
        out.write(denoised_frame)
        phase_latencies["write_output"] += time() - t7
        frame_count += 1

    processing_latency = time() - processing_start
    tr = time()
    video.release()
    if out is not None:
        out.release()
    phase_latencies["resource_release"] = time() - tr
    phase_latencies["function_execution"] = processing_latency
    phase_latencies["processed_frames"] = frame_count
    return processing_latency, result_file_path, phase_latencies


def run_core_profiled(ev):
    latencies = {{}}
    timestamps = {{}}
    timestamps["starting_time"] = time()

    input_bucket = ev["input_bucket"]
    object_key = ev["object_key"]
    output_bucket = ev["output_bucket"]
    endpoint_url = ev["endpoint_url"]
    aws_access_key_id = ev["aws_access_key_id"]
    aws_secret_access_key = ev["aws_secret_access_key"]
    metadata = ev["metadata"]

    t_s3 = time()
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    latencies["s3_client_create"] = time() - t_s3

    download_path = TMP_DIR + "{{}}".format(uuid.uuid4()) + object_key

    t_dl = time()
    s3_client.download_file(input_bucket, object_key, download_path)
    latencies["download_data"] = time() - t_dl

    video_processing_latency, upload_path, phase_lat = video_processing_profiled(object_key, download_path)
    latencies["function_execution"] = video_processing_latency
    latencies["video_processing_phases"] = phase_lat

    t_ul = time()
    s3_client.upload_file(upload_path, output_bucket, upload_path.split("/")[FILE_PATH_INDEX])
    latencies["upload_data"] = time() - t_ul
    timestamps["finishing_time"] = time()
    return {{"latencies": latencies, "timestamps": timestamps, "metadata": metadata}}


with open({event_path!r}, encoding="utf-8") as f:
    ev = json.load(f)
try:
    res = run_core_profiled(ev)
    payload = {{"ok": True, "result": res}}
except Exception as e:
    payload = {{"ok": False, "error": repr(e)}}
with open({core_result_path!r}, "w", encoding="utf-8") as out_f:
    json.dump(payload, out_f)
"""
            sf.write(script)

        cmd = [
            sys.executable,
            "-m",
            "scalene",
            "run",
            "--cpu-only",
            "--cli",
            "--json",
            "--outfile",
            outfile,
            profile_script,
        ]

        child_env = os.environ.copy()
        child_env.setdefault(
            "PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        )

        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=1200,
            env=child_env,
        )

        out = {"scalene_returncode": proc.returncode}
        if include_scalene_stderr:
            out["scalene_stderr"] = proc.stderr or ""
        out["scalene_stdout"] = proc.stdout or ""

        try:
            if core_result_path and os.path.isfile(core_result_path):
                with open(core_result_path, encoding="utf-8") as cr:
                    out["core_result"] = json.load(cr)
            else:
                out["core_result"] = {}
        except Exception:
            out["core_result"] = {}

        try:
            if os.path.isfile(outfile):
                with open(outfile, encoding="utf-8") as pf:
                    raw = pf.read()
                    out["scalene_profile_json_present"] = True
                    if return_full_profile:
                        out["scalene_profile_json"] = raw
                    else:
                        try:
                            profile_obj = json.loads(raw)
                            out["scalene_profile_summary"] = {
                                "elapsed_time_sec": profile_obj.get("elapsed_time_sec"),
                                "program": profile_obj.get("program"),
                                "cpu": profile_obj.get("cpu", True),
                                "gpu": profile_obj.get("gpu", False),
                            }
                        except Exception:
                            out["scalene_profile_json_preview"] = raw
            else:
                out["scalene_profile_json_present"] = False
        except OSError:
            out["scalene_profile_json_present"] = False

        if log_scalene_view and os.path.isfile(outfile):
            try:
                view_cmd = [sys.executable, "-m", "scalene", "view", "--cli", outfile]
                view_proc = subprocess.run(
                    view_cmd,
                    capture_output=True,
                    text=True,
                    timeout=120,
                    env=child_env,
                )
                view_out = (view_proc.stdout or "") + (view_proc.stderr or "")
                view_out = view_out.strip()
                if view_out:
                    print("=== Scalene view (--cli) ===", file=sys.stderr)
                    print(view_out, file=sys.stderr)
            except Exception:
                pass

        if (not out.get("core_result")) and (not out.get("scalene_profile_json_present")):
            out["core_result"] = {"ok": True, "result": _run_core(event)}
        return out
    except Exception as e:
        return {"error": "scalene_wrapper_exception", "exception": repr(e)}
    finally:
        try:
            os.unlink(event_path)
        except OSError:
            pass
        if profile_script:
            try:
                os.unlink(profile_script)
            except OSError:
                pass
        if core_result_path:
            try:
                os.unlink(core_result_path)
            except OSError:
                pass


def main(event):
    event = dict(event)
    use_scalene = bool(event.pop("scalene", False) or event.pop("profile_with_scalene", False))

    if not use_scalene:
        return _run_core(event)

    return _profile_with_scalene(event)


if __name__ == "__main__":
    try:
        if not sys.stdin.isatty():
            _event = json.load(sys.stdin)
        else:
            _event = {}
        result = main(_event)
    except Exception as e:
        result = {"error": "action_exception", "exception": repr(e)}
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")