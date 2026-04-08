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

_PASSTHROUGH_KEYS = (
    "endpoint_url",
    "aws_access_key_id",
    "aws_secret_access_key",
    "output_bucket",
    "metadata",
    "scalene",
    "profile_with_scalene",
    "return_full_scalene_profile",
    "log_scalene_view",
    "include_scalene_stderr",
)


def _process_stage_a(object_key, video_path):
    phase_latencies = {
        "video_open": 0.0,
        "frame_read": 0.0,
        "to_grayscale": 0.0,
        "gaussian_blur": 0.0,
        "median_blur": 0.0,
        "to_bgr": 0.0,
        "writer_open": 0.0,
        "write_output": 0.0,
        "resource_release": 0.0,
    }

    file_name = os.path.splitext(os.path.basename(object_key))[0]
    result_file_path = os.path.join(TMP_DIR, file_name + "-stage-a.avi")

    t_open = time()
    video = cv2.VideoCapture(video_path)
    phase_latencies["video_open"] = time() - t_open
    if not video.isOpened():
        raise RuntimeError("stage_a_open_video_failed")
    fourcc = cv2.VideoWriter_fourcc(*"XVID")
    out = None
    frame_count = 0

    start = time()
    while video.isOpened():
        t_read = time()
        ret, frame = video.read()
        phase_latencies["frame_read"] += time() - t_read
        if not ret:
            break

        t_gray = time()
        gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        phase_latencies["to_grayscale"] += time() - t_gray

        t_g = time()
        gaussian = cv2.GaussianBlur(gray_frame, (5, 5), 0)
        phase_latencies["gaussian_blur"] += time() - t_g

        t_m = time()
        denoised_gray = cv2.medianBlur(gaussian, 5)
        phase_latencies["median_blur"] += time() - t_m

        t_bgr = time()
        denoised_frame = cv2.cvtColor(denoised_gray, cv2.COLOR_GRAY2BGR)
        phase_latencies["to_bgr"] += time() - t_bgr

        if out is None:
            t_writer = time()
            frame_height, frame_width = denoised_frame.shape[:2]
            out = cv2.VideoWriter(
                result_file_path, fourcc, 20.0, (frame_width, frame_height)
            )
            phase_latencies["writer_open"] += time() - t_writer

        t_write = time()
        out.write(denoised_frame)
        phase_latencies["write_output"] += time() - t_write
        frame_count += 1

    latency = time() - start
    t_release = time()
    video.release()
    if out is not None:
        out.release()
    phase_latencies["resource_release"] = time() - t_release
    if frame_count == 0 or (not os.path.isfile(result_file_path)):
        raise RuntimeError("stage_a_no_output_video_generated")
    phase_latencies["processed_frames"] = frame_count
    phase_latencies["function_execution_A"] = latency
    return latency, result_file_path, phase_latencies


def _run_core(event):
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

    stage_a_latency, stage_a_path, stage_a_phases = _process_stage_a(object_key, download_path)
    latencies["function_execution_A"] = stage_a_latency
    latencies["stage_a_phases"] = stage_a_phases

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


def _profile_with_scalene(event):
    outfile = os.path.join(tempfile.gettempdir(), "scalene-video-processing-a.json")
    return_full_profile = bool(event.get("return_full_scalene_profile", False))
    log_scalene_view = bool(event.get("log_scalene_view", True))
    include_scalene_stderr = bool(event.get("include_scalene_stderr", False))

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as ef:
        json.dump(event, ef)
        event_path = ef.name

    core_result_path = None
    profile_script = None
    child_env = os.environ.copy()
    child_env.setdefault("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix="_core_result.json", delete=False, encoding="utf-8") as crf:
            core_result_path = crf.name
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8") as sf:
            profile_script = sf.name
            script = (
                "import json, os, uuid\n"
                "from time import time\n"
                "import boto3, cv2\n"
                "TMP_DIR='/tmp/'\n"
                "def _process_stage_a(object_key, video_path):\n"
                "    phase_latencies={'video_open':0.0,'frame_read':0.0,'to_grayscale':0.0,'gaussian_blur':0.0,'median_blur':0.0,'to_bgr':0.0,'writer_open':0.0,'write_output':0.0,'resource_release':0.0}\n"
                "    file_name=os.path.splitext(os.path.basename(object_key))[0]\n"
                "    result_file_path=os.path.join(TMP_DIR,file_name+'-stage-a.avi')\n"
                "    t_open=time(); video=cv2.VideoCapture(video_path); phase_latencies['video_open']=time()-t_open\n"
                "    if not video.isOpened(): raise RuntimeError('stage_a_open_video_failed')\n"
                "    fourcc=cv2.VideoWriter_fourcc(*'XVID'); out=None; frame_count=0\n"
                "    start=time()\n"
                "    while video.isOpened():\n"
                "        t_read=time(); ret, frame=video.read(); phase_latencies['frame_read']+=time()-t_read\n"
                "        if not ret: break\n"
                "        t_gray=time(); gray_frame=cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY); phase_latencies['to_grayscale']+=time()-t_gray\n"
                "        t_g=time(); gaussian=cv2.GaussianBlur(gray_frame, (5,5), 0); phase_latencies['gaussian_blur']+=time()-t_g\n"
                "        t_m=time(); denoised_gray=cv2.medianBlur(gaussian, 5); phase_latencies['median_blur']+=time()-t_m\n"
                "        t_b=time(); denoised_frame=cv2.cvtColor(denoised_gray, cv2.COLOR_GRAY2BGR); phase_latencies['to_bgr']+=time()-t_b\n"
                "        if out is None:\n"
                "            tw=time(); h,w=denoised_frame.shape[:2]; out=cv2.VideoWriter(result_file_path, fourcc, 20.0, (w,h)); phase_latencies['writer_open']+=time()-tw\n"
                "        twr=time(); out.write(denoised_frame); phase_latencies['write_output']+=time()-twr; frame_count+=1\n"
                "    latency=time()-start\n"
                "    tr=time(); video.release();\n"
                "    if out is not None: out.release()\n"
                "    phase_latencies['resource_release']=time()-tr\n"
                "    if frame_count==0 or (not os.path.isfile(result_file_path)): raise RuntimeError('stage_a_no_output_video_generated')\n"
                "    phase_latencies['processed_frames']=frame_count; phase_latencies['function_execution_A']=latency\n"
                "    return latency, result_file_path, phase_latencies\n"
                "def _run_core(ev):\n"
                "    latencies={}; timestamps={'starting_time':time()}\n"
                "    input_bucket=ev['input_bucket']; object_key=ev['object_key']; output_bucket=ev['output_bucket']\n"
                "    endpoint_url=ev['endpoint_url']; aws_access_key_id=ev['aws_access_key_id']; aws_secret_access_key=ev['aws_secret_access_key']\n"
                "    metadata=ev.get('metadata', {})\n"
                "    s3_client=boto3.client('s3', endpoint_url=endpoint_url, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)\n"
                "    safe_object_name=os.path.basename(object_key); download_path=os.path.join(TMP_DIR, '{}-{}'.format(uuid.uuid4().hex, safe_object_name))\n"
                "    t0=time(); s3_client.download_file(input_bucket, object_key, download_path); latencies['download_data']=time()-t0\n"
                "    l, p, phases=_process_stage_a(object_key, download_path); latencies['function_execution_A']=l; latencies['stage_a_phases']=phases\n"
                "    inter_key='video-processing-intermediate/{}-stage-a.avi'.format(uuid.uuid4().hex)\n"
                "    t1=time(); s3_client.upload_file(p, output_bucket, inter_key); latencies['upload_intermediate']=time()-t1\n"
                "    timestamps['finishing_time']=time()\n"
                "    out={'bucket':output_bucket,'object_key':inter_key,'latencies':latencies,'timestamps':timestamps,'metadata':metadata}\n"
                "    return out\n"
                f"with open({event_path!r}, encoding='utf-8') as f: ev=json.load(f)\n"
                "try:\n"
                "    res=_run_core(ev)\n"
                "    for k in ['endpoint_url','aws_access_key_id','aws_secret_access_key','output_bucket','metadata','scalene','profile_with_scalene','return_full_scalene_profile','log_scalene_view','include_scalene_stderr']:\n"
                "        if k in ev: res[k]=ev[k]\n"
                "    payload={'ok':True,'result':res}\n"
                "except Exception as e:\n"
                "    payload={'ok':False,'error':repr(e)}\n"
                f"with open({core_result_path!r}, 'w', encoding='utf-8') as out_f: json.dump(payload, out_f)\n"
            )
            sf.write(script)

        cmd = [
            sys.executable, "-m", "scalene", "run", "--cpu-only", "--cli", "--json",
            "--outfile", outfile, profile_script,
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=1200, env=child_env)
        out = {"scalene_returncode": proc.returncode, "scalene_stdout": proc.stdout or ""}
        if include_scalene_stderr:
            out["scalene_stderr"] = proc.stderr or ""

        core_payload = {}
        try:
            if core_result_path and os.path.isfile(core_result_path):
                with open(core_result_path, encoding="utf-8") as cr:
                    core_payload = json.load(cr)
        except Exception:
            core_payload = {}

        if core_payload.get("ok") and isinstance(core_payload.get("result"), dict):
            # Keep sequence-compatible shape: top-level bucket/object_key must exist.
            base = dict(core_payload["result"])
        else:
            base = {
                "error": "video_processing_A_scalene_inner_failed",
                "core_result": core_payload,
            }

        if os.path.isfile(outfile):
            with open(outfile, encoding="utf-8") as pf:
                raw = pf.read()
            base["scalene_profile_json_present"] = True
            if return_full_profile:
                base["scalene_profile_json"] = raw
            else:
                try:
                    obj = json.loads(raw)
                    base["scalene_profile_summary"] = {
                        "elapsed_time_sec": obj.get("elapsed_time_sec"),
                        "program": obj.get("program"),
                    }
                except Exception:
                    base["scalene_profile_json_preview"] = raw
        else:
            base["scalene_profile_json_present"] = False

        if log_scalene_view and os.path.isfile(outfile):
            try:
                vp = subprocess.run(
                    [sys.executable, "-m", "scalene", "view", "--cli", outfile],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    env=child_env,
                )
                view_out = ((vp.stdout or "") + (vp.stderr or "")).strip()
                if view_out:
                    print("=== Scalene A view (--cli) ===", file=sys.stderr)
                    print(view_out, file=sys.stderr)
            except Exception:
                pass
        for k, v in out.items():
            base[k] = v
        return base
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
    event = dict(event or {})
    use_scalene = bool(event.get("scalene") or event.get("profile_with_scalene"))
    if use_scalene:
        return _profile_with_scalene(event)
    return _run_core(event)
