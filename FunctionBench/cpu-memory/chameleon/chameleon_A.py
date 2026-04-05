"""
OpenWhisk action A: 编译 Chameleon 模板，将 CHAMELEON_CACHE 打成 zip 上传到 S3 兼容存储（MinIO）。

推荐通过 invocation 传入（与本地 MinIO 配置一致）:
  endpoint_url            例如 "http://172.27.117.185:9000"
  aws_access_key_id       例如 "minioadmin"
  aws_secret_access_key   例如 "minioadmin"
  input_bucket            上传缓存 zip 的 bucket，例如 "input-bucket"（优先使用）

可选:
  output_bucket           未传 input_bucket 时的回退 bucket
  cache_object_prefix     对象名前缀，默认 "chameleon-cache/"
  force_warmup_render     缓存仍为空时是否做一次最小 render

  scalene / profile_with_scalene  为 true 时对 _run_core 做 Scalene 采样（镜像需已安装 scalene）
  行为与 function1.py 一致：子进程执行「内联脚本」（由本文件 _run_core 及依赖函数源码拼成），scalene 只分析该临时 .py，不启用 --profile-all，报告对应 run_core 各阶段行而非 site-packages。
  scalene_reduced_profile        默认 false；为 true 时 scalene run 加 --reduced-profile
  scalene_log_view_max_chars     默认 0：0 或负数表示 scalene view 写入日志时不截断；>0 时只输出前 N 个字符
  log_scalene_view               是否把 scalene view 输出到 activation logs

  # 与 chameleon_B 组成 sequence 一次调用时，请同时传入 B 所需字段，A 会在返回中原样带上：
  num_of_rows, num_of_cols, metadata, repeat_hot_render（以及 scalene 等，便于 B 同样 profiling）

仍支持用环境变量兜底: S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
  INPUT_BUCKET（优先）, OUTPUT_BUCKET
"""
from __future__ import annotations

import inspect
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
import uuid
import warnings
import zipfile
from time import time

# boto3 在 Python 3.9 上会打弃用警告，OpenWhisk 会收进 logs，易误判为错误。
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module=r"boto3\.compat",
)
warnings.filterwarnings(
    "ignore",
    message=".*Boto3 will no longer support Python 3\\.9.*",
    category=DeprecationWarning,
)

import boto3
import six
from botocore.client import Config
from botocore.exceptions import ClientError

# A 成功时把这些键从入参透传到返回值，供 OpenWhisk sequence 中下一步 chameleon_B 使用。
_PASSTHROUGH_TO_B = (
    "endpoint_url",
    "aws_access_key_id",
    "aws_secret_access_key",
    "num_of_rows",
    "num_of_cols",
    "metadata",
    "repeat_hot_render",
    "scalene",
    "profile_with_scalene",
    "log_scalene_view",
    "return_full_scalene_profile",
    "scalene_reduced_profile",
    "scalene_log_view_max_chars",
)

BIGTABLE_ZPT = """\
<table xmlns="http://www.w3.org/1999/xhtml"
xmlns:tal="http://xml.zope.org/namespaces/tal">
<tr tal:repeat="row python: options['table']">
<td tal:repeat="c python: row.values()">
<span tal:define="d python: c + 1"
tal:attributes="class python: 'column-' + %s(d)"
tal:content="python: d" />
</td>
</tr>
</table>""" % six.text_type.__name__


def _strip_s3_expect_100(request, **kwargs):
    """MinIO 等部分 S3 兼容端对 Expect: 100-continue 处理不完整时，urllib3 会报 ConnectionClosedError。"""
    headers = getattr(request, "headers", None)
    if headers is not None and headers.get("Expect"):
        del headers["Expect"]


def _s3_client(event: dict):
    e = dict(event or {})
    url = (e.get("endpoint_url") or os.environ.get("S3_ENDPOINT_URL", "")).strip()
    ak = (e.get("aws_access_key_id") or os.environ.get("AWS_ACCESS_KEY_ID", "")).strip()
    sk = (e.get("aws_secret_access_key") or os.environ.get("AWS_SECRET_ACCESS_KEY", "")).strip()
    if not url or not ak or not sk:
        return None
    # MinIO 通常需要任意 region；不写时部分 boto3 版本会行为异常。
    session = boto3.session.Session()
    client = session.client(
        "s3",
        endpoint_url=url,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )
    client.meta.events.register("before-send.s3", _strip_s3_expect_100)
    return client


def _zip_cache_dir(cache_dir: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(cache_dir):
            for name in files:
                path = os.path.join(root, name)
                arc = os.path.relpath(path, cache_dir)
                zf.write(path, arc)
    return buf.getvalue()


def _cache_nonempty(cache_dir: str) -> bool:
    for _root, _dirs, files in os.walk(cache_dir):
        if files:
            return True
    return False


# Chameleon 在首次 import 时把 CHAMELEON_CACHE 绑定到进程内的 ModuleLoader；若每轮用 mkdtemp 并在
# finally 里 rmtree，warm 容器第二次调用时加载器仍指向已删除路径，会触发 FileNotFoundError（.tmp）。
_STABLE_PT_CACHE = os.path.join(tempfile.gettempdir(), "openwhisk_chameleon_a_pt_cache")


def _reset_pt_cache_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    for name in os.listdir(path):
        p = os.path.join(path, name)
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            else:
                os.unlink(p)
        except OSError:
            pass


def _build_scalene_child_script_a(event_path: str, core_result_path: str) -> str:
    """与 function1.py 相同思路：临时文件里包含完整 _run_core 逻辑，scalene 只统计该文件。"""
    header = (
        "from __future__ import annotations\n"
        "import io\n"
        "import json\n"
        "import os\n"
        "import shutil\n"
        "import tempfile\n"
        "import traceback\n"
        "import uuid\n"
        "import warnings\n"
        "import zipfile\n"
        "from time import time\n"
        "\n"
        "warnings.filterwarnings(\n"
        '    "ignore",\n'
        "    category=DeprecationWarning,\n"
        '    module=r"boto3\\.compat",\n'
        ")\n"
        "warnings.filterwarnings(\n"
        '    "ignore",\n'
        '    message=".*Boto3 will no longer support Python 3\\\\.9.*",\n'
        "    category=DeprecationWarning,\n"
        ")\n"
        "\n"
        "import boto3\n"
        "import six\n"
        "from botocore.client import Config\n"
        "from botocore.exceptions import ClientError\n"
        "\n"
    )
    parts = [
        header,
        f"BIGTABLE_ZPT = {repr(BIGTABLE_ZPT)}\n\n",
        f"_PASSTHROUGH_TO_B = {repr(_PASSTHROUGH_TO_B)}\n\n",
        f"_STABLE_PT_CACHE = {repr(_STABLE_PT_CACHE)}\n\n",
    ]
    for fn in (
        _strip_s3_expect_100,
        _s3_client,
        _zip_cache_dir,
        _cache_nonempty,
        _reset_pt_cache_dir,
        _a_phase_chameleon_compile,
        _a_phase_zip_cache_dir,
        _a_phase_s3_put_object,
        _run_core,
    ):
        parts.append(inspect.getsource(fn))
        parts.append("\n\n")
    parts.append(
        f"with open({event_path!r}, encoding='utf-8') as _f:\n"
        f"    _ev = json.load(_f)\n"
        "try:\n"
        "    _r = _run_core(dict(_ev or {}))\n"
        "    _payload = {'ok': True, 'result': _r}\n"
        "except Exception as _e:\n"
        "    _payload = {'ok': False, 'error': repr(_e)}\n"
        f"with open({core_result_path!r}, 'w', encoding='utf-8') as _out:\n"
        "    json.dump(_payload, _out)\n"
    )
    return "".join(parts)


def _profile_with_scalene_a(action_path: str, event: dict) -> dict:
    """子进程跑 scalene，仅统计 _run_core；返回体形状与正常 A 一致，便于 sequence 接 B。"""
    outfile = os.path.join(tempfile.gettempdir(), "scalene-chameleon-a.json")
    return_full = bool(event.get("return_full_scalene_profile", False))
    log_view = bool(event.get("log_scalene_view", True))

    ev_clean = dict(event)
    for k in (
        "scalene",
        "profile_with_scalene",
        "return_full_scalene_profile",
        "log_scalene_view",
        "scalene_reduced_profile",
        "scalene_log_view_max_chars",
    ):
        ev_clean.pop(k, None)

    event_path = None
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
        encoding="utf-8",
    ) as ef:
        event_path = ef.name
        json.dump(ev_clean, ef)

    core_result_path = None
    profile_script = None
    child_env = os.environ.copy()
    child_env.setdefault(
        "PATH",
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    )

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix="_core.json",
            delete=False,
            encoding="utf-8",
        ) as crf:
            core_result_path = crf.name

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".py",
            delete=False,
            encoding="utf-8",
        ) as sf:
            profile_script = sf.name
            sf.write(_build_scalene_child_script_a(event_path, core_result_path))

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
        ]
        if bool(event.get("scalene_reduced_profile", False)):
            cmd.append("--reduced-profile")
        cmd.append(profile_script)
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=600,
            env=child_env,
        )

        extra = {
            "scalene_returncode": proc.returncode,
            "scalene_stdout_preview": proc.stdout or "",
            "scalene_stderr": proc.stderr or "",
        }

        base: dict = {}
        try:
            if core_result_path and os.path.isfile(core_result_path):
                with open(core_result_path, encoding="utf-8") as cr:
                    payload = json.load(cr)
                if payload.get("ok") and isinstance(payload.get("result"), dict):
                    base = dict(payload["result"])
                else:
                    base = {
                        "error": "chameleon_A_scalene_inner_failed",
                        "inner": payload,
                    }
        except Exception as e:
            base = {"error": "chameleon_A_scalene_parse_failed", "exception": repr(e)}

        if os.path.isfile(outfile):
            try:
                with open(outfile, encoding="utf-8") as pf:
                    raw = pf.read()
                if return_full:
                    base["scalene_profile_json"] = raw
                else:
                    try:
                        po = json.loads(raw)
                        base["scalene_profile_summary"] = {
                            "elapsed_time_sec": po.get("elapsed_time_sec"),
                            "program": po.get("program"),
                        }
                    except Exception:
                        base["scalene_profile_json_preview"] = raw
            except OSError:
                pass

        if log_view and os.path.isfile(outfile):
            try:
                view_proc = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "scalene",
                        "view",
                        "--cli",
                        outfile,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    env=child_env,
                )
                vo = ((view_proc.stdout or "") + (view_proc.stderr or "")).strip()
                if vo:
                    lim_raw = event.get("scalene_log_view_max_chars", 0)
                    max_chars = int(lim_raw) if lim_raw is not None else 0
                    if max_chars > 0:
                        out = vo[:max_chars]
                        title = (
                            f"=== Scalene A inline _run_core (--cli, first {max_chars} chars) ==="
                        )
                    else:
                        out = vo
                        title = "=== Scalene A inline _run_core (--cli, full) ==="
                    print(title, file=sys.stderr)
                    print(out, file=sys.stderr)
            except Exception:
                pass

        for k, v in extra.items():
            base[k] = v
        for k in _PASSTHROUGH_TO_B:
            if k in event:
                base[k] = event[k]
        return base
    finally:
        try:
            if event_path:
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


def _a_phase_chameleon_compile(
    cache_dir: str, force_warmup: bool, latencies: dict[str, float]
):
    """Scalene 下会单独占行，便于区分模板编译/预热与 zip/S3。"""
    os.environ["CHAMELEON_CACHE"] = cache_dir
    os.environ["CHAMELEON_EAGER"] = "1"
    t0 = time()
    from chameleon import PageTemplate  # noqa: WPS433

    tmpl = PageTemplate(BIGTABLE_ZPT)
    latencies["compile_instantiate"] = time() - t0
    if not _cache_nonempty(cache_dir) and force_warmup:
        tw = time()
        _ = tmpl.render(options={"table": []})
        latencies["warmup_render"] = time() - tw
    return tmpl


def _a_phase_zip_cache_dir(cache_dir: str, latencies: dict[str, float]) -> bytes:
    tz = time()
    payload = _zip_cache_dir(cache_dir)
    latencies["zip_cache"] = time() - tz
    return payload


def _a_phase_s3_put_object(
    client,
    bucket: str,
    object_key: str,
    payload: bytes,
    latencies: dict[str, float],
) -> None:
    tu = time()
    client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=payload,
        ContentType="application/zip",
    )
    latencies["s3_upload"] = time() - tu


def _run_core(event):
    event = dict(event or {})
    latencies: dict[str, float] = {}
    timestamps = {"starting_time": time()}

    bucket = (
        event.get("input_bucket")
        or event.get("output_bucket")
        or event.get("minio_bucket")
        or os.environ.get("INPUT_BUCKET", "")
        or os.environ.get("OUTPUT_BUCKET", "")
    ).strip()
    prefix = (event.get("cache_object_prefix") or "chameleon-cache/").strip()
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    force_warmup = bool(event.get("force_warmup_render", False))

    client = _s3_client(event)
    if client is None or not bucket:
        return {
            "error": "missing_storage_config",
            "hint": "Pass endpoint_url, aws_access_key_id, aws_secret_access_key, input_bucket (or output_bucket)",
        }

    cache_dir = _STABLE_PT_CACHE
    _reset_pt_cache_dir(cache_dir)

    try:
        _a_phase_chameleon_compile(cache_dir, force_warmup, latencies)

        if not _cache_nonempty(cache_dir):
            return {
                "error": "chameleon_cache_empty",
                "hint": "Try force_warmup_render=true",
                "latencies": latencies,
                "cache_dir_listing": os.listdir(cache_dir),
            }

        payload = _a_phase_zip_cache_dir(cache_dir, latencies)

        object_key = f"{prefix}{uuid.uuid4().hex}.zip"

        try:
            _a_phase_s3_put_object(client, bucket, object_key, payload, latencies)
        except ClientError as e:
            err = e.response.get("Error", {}) if e.response else {}
            return {
                "error": "chameleon_A_s3_put_failed",
                "s3_code": err.get("Code"),
                "s3_message": err.get("Message"),
                "bucket": bucket,
                "object_key": object_key,
                "exception": repr(e),
            }

        timestamps["finishing_time"] = time()
        out = {
            "ok": True,
            "bucket": bucket,
            "object_key": object_key,
            "cache_zip_bytes": len(payload),
            "latencies": latencies,
            "timestamps": timestamps,
            "metadata": event.get("metadata", {}),
        }
        for k in _PASSTHROUGH_TO_B:
            if k in event:
                out[k] = event[k]
        return out
    except Exception as e:
        tb = traceback.format_exc()
        # 部分 OpenWhisk/控制台只展示 result["error"]，把摘要塞进 error 便于直接看到原因。
        detail = f"{type(e).__name__}: {e}"
        err_msg = f"chameleon_A_failed: {detail}"
        try:
            print(err_msg, file=sys.stderr)
            print(tb[-8000:], file=sys.stderr)
        except Exception:
            pass
        return {
            "error": err_msg,
            "error_code": "chameleon_A_failed",
            "exception": repr(e),
            "traceback": tb[-4000:],
            "hint": "常见原因: 容器访问不到 MinIO 地址; bucket 不存在; 缺 boto3/chameleon",
        }


def main(event):
    event = dict(event or {})
    if bool(event.get("scalene") or event.get("profile_with_scalene")):
        return _profile_with_scalene_a(os.path.abspath(__file__), event)
    return _run_core(event)
