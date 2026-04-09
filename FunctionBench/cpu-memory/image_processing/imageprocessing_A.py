import json
import os
import subprocess
import sys
import tempfile
import uuid
from time import time

import boto3
from PIL import Image

import ops

TMP_DIR = "/tmp/"
FILE_NAME_INDEX = 1


def image_processing_a(file_name, image_path):
    phase_latencies = {
        "open_image": 0.0,
        "flip": 0.0,
        "rotate": 0.0,
        "gray_scale": 0.0,
        "resize": 0.0,
    }
    path_list = []
    start = time()

    t0 = time()
    with Image.open(image_path) as image:
        phase_latencies["open_image"] = time() - t0

        t1 = time()
        path_list += ops.flip(image, file_name)
        phase_latencies["flip"] = time() - t1

        t2 = time()
        path_list += ops.rotate(image, file_name)
        phase_latencies["rotate"] = time() - t2

        t3 = time()
        path_list += ops.gray_scale(image, file_name)
        phase_latencies["gray_scale"] = time() - t3

        t4 = time()
        path_list += ops.resize(image, file_name)
        phase_latencies["resize"] = time() - t4

    latency = time() - start
    phase_latencies["function_execution_A"] = latency
    return latency, path_list, phase_latencies


def _run_core(event):
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

    start = time()
    download_path = TMP_DIR + "{}{}".format(uuid.uuid4(), object_key)
    s3_client.download_file(input_bucket, object_key, download_path)
    latencies["download_data_A"] = time() - start

    exec_latency, path_list, phases = image_processing_a(object_key, download_path)
    latencies["function_execution_A"] = exec_latency
    latencies["image_processing_phases_A"] = phases

    start = time()
    for upload_path in path_list:
        s3_client.upload_file(
            upload_path, output_bucket, upload_path.split("/")[FILE_NAME_INDEX]
        )
    latencies["upload_data_A"] = time() - start
    timestamps["finishing_time"] = time()

    out_event = dict(event)
    out_event["latencies"] = {**event.get("latencies", {}), **latencies}
    out_event["timestamps_A"] = timestamps
    out_event["metadata"] = metadata
    return out_event


def _profile_with_scalene(event):
    outfile = os.path.join(tempfile.gettempdir(), "scalene-image-processing-a.json")
    log_scalene_view = bool(event.get("log_scalene_view", True))
    include_scalene_stderr = bool(event.get("include_scalene_stderr", False))

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as ef:
        json.dump(event, ef)
        event_path = ef.name

    core_result_path = None
    profile_script = None
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
from PIL import Image, ImageFilter

TMP_DIR = "/tmp/"
FILE_NAME_INDEX = 1


def image_processing_a_profiled(file_name, image_path):
    phase_latencies = {{
        "open_image": 0.0,
        "flip_left_right": 0.0,
        "flip_top_bottom": 0.0,
        "rotate_90": 0.0,
        "rotate_180": 0.0,
        "rotate_270": 0.0,
        "gray_scale": 0.0,
        "resize": 0.0,
    }}
    path_list = []
    t0 = time()
    with Image.open(image_path) as image:
        phase_latencies["open_image"] = time() - t0

        t = time()
        p = "./flip-left-right-" + file_name
        img = image.transpose(Image.FLIP_LEFT_RIGHT)
        img.save(p)
        path_list.append(p)
        phase_latencies["flip_left_right"] += time() - t

        t = time()
        p = "./flip-top-bottom-" + file_name
        img = image.transpose(Image.FLIP_TOP_BOTTOM)
        img.save(p)
        path_list.append(p)
        phase_latencies["flip_top_bottom"] += time() - t

        t = time()
        p = "./rotate-90-" + file_name
        img = image.transpose(Image.ROTATE_90)
        img.save(p)
        path_list.append(p)
        phase_latencies["rotate_90"] += time() - t

        t = time()
        p = "./rotate-180-" + file_name
        img = image.transpose(Image.ROTATE_180)
        img.save(p)
        path_list.append(p)
        phase_latencies["rotate_180"] += time() - t

        t = time()
        p = "./rotate-270-" + file_name
        img = image.transpose(Image.ROTATE_270)
        img.save(p)
        path_list.append(p)
        phase_latencies["rotate_270"] += time() - t

        t = time()
        p = "./gray-scale-" + file_name
        img = image.convert("L")
        img.save(p)
        path_list.append(p)
        phase_latencies["gray_scale"] += time() - t

        t = time()
        p = "./resized-" + file_name
        resized = image.copy()
        resized.thumbnail((128, 128))
        resized.save(p)
        path_list.append(p)
        phase_latencies["resize"] += time() - t

    processing_latency = sum(phase_latencies.values())
    phase_latencies["function_execution_A"] = processing_latency
    return processing_latency, path_list, phase_latencies


def run_core_profiled(ev):
    latencies = {{}}
    timestamps = {{"starting_time": time()}}

    input_bucket = ev["input_bucket"]
    object_key = ev["object_key"]
    output_bucket = ev["output_bucket"]
    endpoint_url = ev["endpoint_url"]
    aws_access_key_id = ev["aws_access_key_id"]
    aws_secret_access_key = ev["aws_secret_access_key"]
    metadata = ev.get("metadata", {{}})

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    download_path = TMP_DIR + "{{}}".format(uuid.uuid4()) + object_key

    t_dl = time()
    s3_client.download_file(input_bucket, object_key, download_path)
    latencies["download_data_A"] = time() - t_dl

    exec_latency, path_list, phases = image_processing_a_profiled(object_key, download_path)
    latencies["function_execution_A"] = exec_latency
    latencies["image_processing_phases_A"] = phases

    t_ul = time()
    for upload_path in path_list:
        s3_client.upload_file(upload_path, output_bucket, upload_path.split("/")[FILE_NAME_INDEX])
    latencies["upload_data_A"] = time() - t_ul
    timestamps["finishing_time"] = time()

    out_event = dict(ev)
    out_event["latencies"] = {{**(ev.get("latencies") or {{}}), **latencies}}
    out_event["timestamps_A"] = timestamps
    out_event["metadata"] = metadata
    return out_event


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
                    try:
                        profile_obj = json.loads(raw)
                        out["scalene_profile_summary"] = {
                            "elapsed_time_sec": profile_obj.get("elapsed_time_sec"),
                            "program": profile_obj.get("program"),
                            "cpu": profile_obj.get("cpu", True),
                            "gpu": profile_obj.get("gpu", False),
                        }
                    except Exception:
                        out["scalene_profile_summary"] = {
                            "parse_error": True,
                            "raw_size_bytes": len(raw),
                        }
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
                    print("=== Scalene imageprocessing_A (--cli) ===", file=sys.stderr)
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
    forward_scalene = bool(event.get("scalene") or event.get("profile_with_scalene"))
    use_scalene = bool(event.pop("scalene", False) or event.pop("profile_with_scalene", False))

    if not use_scalene:
        return _run_core(event)

    prof = _profile_with_scalene(event)
    core = prof.get("core_result") or {}
    if core.get("ok") and isinstance(core.get("result"), dict):
        merged = dict(core["result"])
        merged["scalene_trace_A"] = {
            "scalene_returncode": prof.get("scalene_returncode"),
            "scalene_profile_summary": prof.get("scalene_profile_summary"),
            "scalene_profile_json_present": prof.get("scalene_profile_json_present"),
        }
        if forward_scalene:
            merged["scalene"] = True
        return merged
    return prof


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
