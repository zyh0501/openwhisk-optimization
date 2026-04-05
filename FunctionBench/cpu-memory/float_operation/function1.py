import math
from time import time
import json
import os
import sys
import subprocess
import tempfile

# 直接依赖 scalene（镜像里应已安装）
try:
    SCALENE_AVAILABLE = True
    import scalene  # noqa: F401
    print("Scalene import OK")
except ImportError as e:
    SCALENE_AVAILABLE = False
    print(f"Scalene not available: {e}")

def float_operations(n):
    start = time()
    for i in range(0, n):
        sin_i = math.sin(i)
        cos_i = math.cos(i)
        sqrt_i = math.sqrt(i)
    latency = time() - start
    return latency

def run_scalene_for_float_operations(n: int):
    """
    使用 `python -m scalene` 启动一个子进程来跑纯计算目标。

    原因：在某些 scalene 版本/启动方式下，直接在进程内调用
    `scalene_profiler.enable_profiling()` 会出现未初始化报错。
    """
    run_dir = tempfile.mkdtemp(prefix="scalene_float_")
    latency_path = os.path.join(run_dir, "latency.txt")
    target_script_path = os.path.join(run_dir, "scalene_target.py")
    profile_outfile_base = os.path.join(run_dir, "stdout")
    profile_json_path = f"{profile_outfile_base}.json"

    # 只做计算，确保 scalene 覆盖到目标代码段。
    # 另外提供 warmup_iters：用来保证 scalene 至少采样到足够长的运行时间
    #（Scalene 可能要求至少 1 秒或至少 10MB 分配）。
    target_script = r'''
import math
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--n", type=int, required=True)
parser.add_argument("--warmup_iters", type=int, required=True)
parser.add_argument("--latency_path", type=str, required=True)
args = parser.parse_args()

for i in range(0, args.warmup_iters):
    math.sin(i)
    math.cos(i)
    math.sqrt(i)

start = time.time()
for i in range(0, args.n):
    math.sin(i)
    math.cos(i)
    math.sqrt(i)

latency = time.time() - start
with open(args.latency_path, "w", encoding="utf-8") as f:
    f.write(str(latency))
'''

    with open(target_script_path, "w", encoding="utf-8") as f:
        f.write(target_script.lstrip("\n"))

    # In OpenWhisk action runtime, some environment variables may be missing.
    # Scalene's redirect_python expects `PATH` to exist in this process.
    env = os.environ.copy()
    default_path = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    if env.get("PATH") or env.get("Path"):
        env["PATH"] = env.get("PATH") or env.get("Path")
    else:
        env["PATH"] = default_path

    # Also patch os.environ in the parent (scalene may consult parent process env)
    os.environ["PATH"] = env["PATH"]

    # scalene v2 的 CLI 参数在不同小版本间存在差异。
    # 为了提高在 OpenWhisk runtime 中的成功率，这里用多个候选命令重试：
    # 目标是让 Scalene 成功接管 options，并让 target_script.py 只接收到
    # `--n` 和 `--latency_path`。
    cmd_base_candidates = [
        [
            sys.executable,
            "-m",
            "scalene",
            "run",
            "--outfile",
            profile_outfile_base,
            "--cpu-only",
            target_script_path,
            "---",
        ],
        [
            sys.executable,
            "-m",
            "scalene",
            "run",
            "--outfile",
            profile_outfile_base,
            target_script_path,
            "---",
        ],
        [
            sys.executable,
            "-m",
            "scalene",
            "run",
            "--outfile",
            profile_outfile_base,
            "--cpu-only",
            target_script_path,
            "--",
        ],
    ]

    last_output = ""
    last_rc = None
    # Quick sanity check for env propagation (appears in action stdout).
    # Note: this is for debugging; keep output truncated.
    try:
        parent_path_present = ("PATH" in os.environ)
        parent_path_val = os.environ.get("PATH") or os.environ.get("Path") or ""
        print(
            f"[debug] parent PATH present={parent_path_present} len={len(parent_path_val)}"
        )
        print(
            f"[debug] env PATH len={len(env.get('PATH',''))} parent PATH now len={len(os.environ.get('PATH',''))}"
        )
    except Exception:
        pass

    # warmup_iters 逐步增加，避免 scalene 提示 "did not run for long enough"
    for warmup_iters in (0, n, n * 2):
        for cmd_base in cmd_base_candidates:
            cmd = cmd_base + [
                "--n",
                str(n),
                "--warmup_iters",
                str(warmup_iters),
                "--latency_path",
                latency_path,
            ]

            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                env=env,
                cwd=run_dir,
            )
            last_rc = completed.returncode
            last_output = (completed.stdout or "") + (completed.stderr or "")

            try:
                with open(latency_path, "r", encoding="utf-8") as f:
                    latency = float(f.read().strip())
                print(
                    f"[debug] latency read OK latency={latency} rc={last_rc} path={latency_path}"
                )

                # Print Scalene profile to stdout (so you don't need to fetch stdout.json from container).
                try:
                    if os.path.exists(profile_json_path):
                        view_cmd = [
                            sys.executable,
                            "-m",
                            "scalene",
                            "view",
                            "--cli",
                            profile_json_path,
                        ]
                        view_completed = subprocess.run(
                            view_cmd,
                            capture_output=True,
                            text=True,
                            check=False,
                            env=env,
                            cwd=run_dir,
                        )
                        view_out = (view_completed.stdout or "") + (
                            view_completed.stderr or ""
                        )
                        if view_out:
                            print("=== Scalene view (--cli) ===")
                            print(view_out[:8000])
                except Exception:
                    pass

                return latency, last_rc, last_output
            except Exception:
                continue

    return None, last_rc, last_output

def main(event):
    latencies = {}
    timestamps = {}
    timestamps["starting_time"] = time()
    
    n = int(event.get('n', 1000000))
    metadata = event.get('metadata', {})
    profile_mode = event.get('profile_mode', 'none')
    
    result = {
        "latencies": latencies,
        "timestamps": timestamps,
        "metadata": metadata,
        "n": n
    }
    
    try:
        if profile_mode in ("full", "block") and SCALENE_AVAILABLE:
            print(f"Starting profiling with mode: {profile_mode}")

            latency, rc, scalene_output = run_scalene_for_float_operations(n)
            if latency is None:
                # 兜底：如果子进程没写出 latency，则退回正常执行
                latency = float_operations(n)
                print("Warning: scalene target did not write latency, fell back.")

            latencies["function_execution"] = latency
            result["profiling"] = "completed"
            result["scalene_return_code"] = rc

            # 让调用方能在 activation logs 里看到核心输出（避免 response 过大）
            if scalene_output:
                preview = scalene_output[:4000]
                print("=== Scalene Output (truncated) ===")
                print(preview)
        else:
            # 正常执行
            latency = float_operations(n)
            latencies["function_execution"] = latency
        
        timestamps["finishing_time"] = time()
        result["total_time"] = timestamps["finishing_time"] - timestamps["starting_time"]
        
        return result
        
    except Exception as e:
        return {"error": str(e), "profile_mode": profile_mode}
    
