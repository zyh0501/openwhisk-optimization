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


def _process_stage_b(object_key, video_path):
    phase_latencies = {
        "video_open": 0.0,
        "frame_read": 0.0,
        "imwrite_jpeg": 0.0,
        "imread_jpeg": 0.0,
        "writer_open": 0.0,
        "write_output": 0.0,
        "resource_release": 0.0,
    }
    file_name = os.path.splitext(os.path.basename(object_key))[0]
    result_file_path = os.path.join(TMP_DIR, file_name + "-stage-b.avi")
    tmp_file_path = os.path.join(TMP_DIR, "tmp.jpg")

    t_open = time()
    video = cv2.VideoCapture(video_path)
    phase_latencies["video_open"] = time() - t_open
    if not video.isOpened():
        raise RuntimeError("stage_b_open_video_failed")
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

        t_w = time()
        cv2.imwrite(tmp_file_path, frame)
        phase_latencies["imwrite_jpeg"] += time() - t_w

        t_r = time()
        jpg_frame = cv2.imread(tmp_file_path)
        phase_latencies["imread_jpeg"] += time() - t_r

        if out is None:
            t_o = time()
            frame_height, frame_width = jpg_frame.shape[:2]
            out = cv2.VideoWriter(result_file_path, fourcc, 20.0, (frame_width, frame_height))
            phase_latencies["writer_open"] += time() - t_o

        t_out = time()
        out.write(jpg_frame)
        phase_latencies["write_output"] += time() - t_out
        frame_count += 1

    latency = time() - start
    t_release = time()
    video.release()
    if out is not None:
        out.release()
    phase_latencies["resource_release"] = time() - t_release
    if frame_count == 0 or (not os.path.isfile(result_file_path)):
        raise RuntimeError("stage_b_no_output_video_generated")
    phase_latencies["processed_frames"] = frame_count
    phase_latencies["function_execution_B"] = latency
    return latency, result_file_path, phase_latencies


def _run_core(event):
    event = dict(event or {})
    latencies = {}
    timestamps = {"starting_time": time()}

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

    stage_b_latency, upload_path, stage_b_phases = _process_stage_b(object_key, download_path)
    latencies["function_execution_B"] = stage_b_latency
    latencies["stage_b_phases"] = stage_b_phases

    final_key = os.path.basename(upload_path)
    t1 = time()
    s3_client.upload_file(upload_path, output_bucket, final_key)
    latencies["upload_data"] = time() - t1

    timestamps["finishing_time"] = time()
    return {
        "latencies": latencies,
        "timestamps": timestamps,
        "metadata": metadata,
        "final_object_key": final_key,
    }


def _profile_with_scalene(event):
    outfile = os.path.join(tempfile.gettempdir(), "scalene-video-processing-b.json")
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
                "def _process_stage_b(object_key, video_path):\n"
                "    phase_latencies={'video_open':0.0,'frame_read':0.0,'imwrite_jpeg':0.0,'imread_jpeg':0.0,'writer_open':0.0,'write_output':0.0,'resource_release':0.0}\n"
                "    file_name=os.path.splitext(os.path.basename(object_key))[0]\n"
                "    result_file_path=os.path.join(TMP_DIR,file_name+'-stage-b.avi'); tmp_file_path=os.path.join(TMP_DIR,'tmp.jpg')\n"
                "    t_open=time(); video=cv2.VideoCapture(video_path); phase_latencies['video_open']=time()-t_open\n"
                "    if not video.isOpened(): raise RuntimeError('stage_b_open_video_failed')\n"
                "    fourcc=cv2.VideoWriter_fourcc(*'XVID'); out=None; frame_count=0\n"
                "    start=time()\n"
                "    while video.isOpened():\n"
                "        t_read=time(); ret, frame=video.read(); phase_latencies['frame_read']+=time()-t_read\n"
                "        if not ret: break\n"
                "        t_w=time(); cv2.imwrite(tmp_file_path, frame); phase_latencies['imwrite_jpeg']+=time()-t_w\n"
                "        t_r=time(); jpg_frame=cv2.imread(tmp_file_path); phase_latencies['imread_jpeg']+=time()-t_r\n"
                "        if out is None:\n"
                "            t_o=time(); h,w=jpg_frame.shape[:2]; out=cv2.VideoWriter(result_file_path, fourcc, 20.0, (w,h)); phase_latencies['writer_open']+=time()-t_o\n"
                "        t_out=time(); out.write(jpg_frame); phase_latencies['write_output']+=time()-t_out; frame_count+=1\n"
                "    latency=time()-start\n"
                "    tr=time(); video.release();\n"
                "    if out is not None: out.release()\n"
                "    phase_latencies['resource_release']=time()-tr\n"
                "    if frame_count==0 or (not os.path.isfile(result_file_path)): raise RuntimeError('stage_b_no_output_video_generated')\n"
                "    phase_latencies['processed_frames']=frame_count; phase_latencies['function_execution_B']=latency\n"
                "    return latency, result_file_path, phase_latencies\n"
                "def _run_core(ev):\n"
                "    latencies={}; timestamps={'starting_time':time()}\n"
                "    input_bucket=ev.get('bucket') or ev.get('input_bucket')\n"
                "    object_key=ev.get('object_key') or ev.get('intermediate_object_key') or ev.get('key')\n"
                "    if (not input_bucket) or (not object_key):\n"
                "        return {'error':'missing_intermediate_location','hint':'videoproc-b needs bucket/input_bucket and object_key (or intermediate_object_key/key).','received_keys':sorted(list(ev.keys()))}\n"
                "    output_bucket=ev['output_bucket']\n"
                "    endpoint_url=ev['endpoint_url']; aws_access_key_id=ev['aws_access_key_id']; aws_secret_access_key=ev['aws_secret_access_key']; metadata=ev.get('metadata', {})\n"
                "    s3_client=boto3.client('s3', endpoint_url=endpoint_url, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)\n"
                "    safe_object_name=os.path.basename(object_key); download_path=os.path.join(TMP_DIR, '{}-{}'.format(uuid.uuid4().hex, safe_object_name))\n"
                "    t0=time(); s3_client.download_file(input_bucket, object_key, download_path); latencies['download_intermediate']=time()-t0\n"
                "    l, p, phases=_process_stage_b(object_key, download_path); latencies['function_execution_B']=l; latencies['stage_b_phases']=phases\n"
                "    final_key=os.path.basename(p); t1=time(); s3_client.upload_file(p, output_bucket, final_key); latencies['upload_data']=time()-t1\n"
                "    timestamps['finishing_time']=time()\n"
                "    return {'latencies':latencies,'timestamps':timestamps,'metadata':metadata,'final_object_key':final_key}\n"
                f"with open({event_path!r}, encoding='utf-8') as f: ev=json.load(f)\n"
                "try:\n"
                "    payload={'ok':True,'result':_run_core(ev)}\n"
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

        try:
            if core_result_path and os.path.isfile(core_result_path):
                with open(core_result_path, encoding="utf-8") as cr:
                    out["core_result"] = json.load(cr)
        except Exception:
            out["core_result"] = {}

        if os.path.isfile(outfile):
            with open(outfile, encoding="utf-8") as pf:
                raw = pf.read()
            out["scalene_profile_json_present"] = True
            if return_full_profile:
                out["scalene_profile_json"] = raw
            else:
                try:
                    obj = json.loads(raw)
                    out["scalene_profile_summary"] = {
                        "elapsed_time_sec": obj.get("elapsed_time_sec"),
                        "program": obj.get("program"),
                    }
                except Exception:
                    out["scalene_profile_json_preview"] = raw
        else:
            out["scalene_profile_json_present"] = False

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
                    print("=== Scalene B view (--cli) ===", file=sys.stderr)
                    print(view_out, file=sys.stderr)
            except Exception:
                pass
        return out
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
