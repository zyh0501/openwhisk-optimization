"""
OpenWhisk action B: 从 S3 兼容存储（MinIO）下载 A 上传的 zip，解压后 CHAMELEON_CACHE 下渲染。

invocation 需传入（与 A 一致）:
  endpoint_url, aws_access_key_id, aws_secret_access_key
  bucket + object_key   （即 A 返回的 bucket 与 object_key，现多为 input-bucket）
  也可用 minio_object: "bucket/key"

  num_of_rows, num_of_cols, metadata
  repeat_hot_render     可选，默认 5

  scalene / profile_with_scalene  为 true 时用子进程跑 scalene（需镜像已安装 scalene）
  与 function1.py 一致：内联临时脚本包含 _run_core 及依赖（inspect 拼源码），scalene 只分析该文件，不用 --profile-all。
  scalene_reduced_profile        默认 false；为 true 时加 --reduced-profile
  scalene_log_view_max_chars     默认 0：0 或负数表示 view 全文写入日志；>0 时只输出前 N 字符
  log_scalene_view               是否把 scalene view 打到 activation logs（stderr）

若未传 bucket，则尝试 output_bucket 或 input_bucket。
"""
from __future__ import annotations

import inspect
import json
import os
import shutil
import subprocess
import sys
import tempfile
import warnings
import zipfile
from time import time

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
    session = boto3.session.Session()
    client = session.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )
    client.meta.events.register("before-send.s3", _strip_s3_expect_100)
    return client


_STABLE_PT_CACHE = os.path.join(tempfile.gettempdir(), "openwhisk_chameleon_b_pt_cache")


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


def _build_scalene_child_script_b(event_path: str, core_result_path: str) -> str:
    header = (
        "from __future__ import annotations\n"
        "import json\n"
        "import os\n"
        "import shutil\n"
        "import tempfile\n"
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
        "\n"
    )
    parts = [
        header,
        f"BIGTABLE_ZPT = {repr(BIGTABLE_ZPT)}\n\n",
        f"_STABLE_PT_CACHE = {repr(_STABLE_PT_CACHE)}\n\n",
    ]
    for fn in (
        _strip_s3_expect_100,
        _s3_client,
        _reset_pt_cache_dir,
        _b_phase_s3_get_to_file,
        _b_phase_unzip_to_cache,
        _b_phase_remove_zip,
        _b_phase_page_template,
        _b_phase_build_options,
        _b_phase_first_render,
        _b_phase_hot_renders,
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


def _profile_with_scalene_b(action_path: str, event: dict) -> dict:
    outfile = os.path.join(tempfile.gettempdir(), "scalene-chameleon-b.json")
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
            sf.write(_build_scalene_child_script_b(event_path, core_result_path))

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
                        "error": "chameleon_B_scalene_inner_failed",
                        "inner": payload,
                    }
        except Exception as e:
            base = {"error": "chameleon_B_scalene_parse_failed", "exception": repr(e)}

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
                            f"=== Scalene B inline _run_core (--cli, first {max_chars} chars) ==="
                        )
                    else:
                        out = vo
                        title = "=== Scalene B inline _run_core (--cli, full) ==="
                    print(title, file=sys.stderr)
                    print(out, file=sys.stderr)
            except Exception:
                pass

        for k, v in extra.items():
            base[k] = v
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


def _b_phase_s3_get_to_file(
    client,
    bucket: str,
    object_key: str,
    zip_path: str,
    latencies: dict[str, float],
) -> None:
    t_dl = time()
    obj = client.get_object(Bucket=bucket, Key=object_key)
    data = obj["Body"].read()
    with open(zip_path, "wb") as f:
        f.write(data)
    latencies["s3_download"] = time() - t_dl


def _b_phase_unzip_to_cache(
    zip_path: str, cache_dir: str, latencies: dict[str, float]
) -> None:
    t_unzip = time()
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(cache_dir)
    latencies["unzip_cache"] = time() - t_unzip


def _b_phase_remove_zip(zip_path: str) -> None:
    try:
        os.remove(zip_path)
    except OSError:
        pass


def _b_phase_page_template(cache_dir: str, latencies: dict[str, float]):
    os.environ["CHAMELEON_CACHE"] = cache_dir
    os.environ.pop("CHAMELEON_EAGER", None)
    from chameleon import PageTemplate  # noqa: WPS433

    t0 = time()
    tmpl = PageTemplate(BIGTABLE_ZPT)
    latencies["template_object_create"] = time() - t0
    return tmpl


def _b_phase_build_options(
    num_rows: int, num_cols: int, latencies: dict[str, float]
) -> dict:
    t1 = time()
    data = {}
    for i in range(num_cols):
        data[str(i)] = i
    table = [data for _ in range(num_rows)]
    options = {"table": table}
    latencies["input_build"] = time() - t1
    return options


def _b_phase_first_render(
    tmpl, options: dict, latencies: dict[str, float]
) -> None:
    t2 = time()
    _ = tmpl.render(options=options)
    latencies["first_render"] = time() - t2


def _b_phase_hot_renders(
    tmpl,
    options: dict,
    repeat_hot_render: int,
    latencies: dict[str, float],
) -> None:
    t3 = time()
    for _ in range(repeat_hot_render):
        _ = tmpl.render(options=options)
    latencies["hot_render_total"] = time() - t3
    latencies["hot_render_avg"] = latencies["hot_render_total"] / repeat_hot_render


def _run_core(event):
    event = dict(event or {})
    latencies: dict[str, float] = {}
    timestamps = {"starting_time": time()}

    bucket = (
        event.get("bucket")
        or event.get("input_bucket")
        or event.get("output_bucket")
        or ""
    ).strip()
    object_key = (event.get("object_key") or "").strip()
    if not bucket or not object_key:
        mo = (event.get("minio_object") or "").strip()
        if "/" in mo:
            bucket, object_key = mo.split("/", 1)
            bucket, object_key = bucket.strip(), object_key.strip()

    num_of_rows = int(event["num_of_rows"])
    num_of_cols = int(event["num_of_cols"])
    metadata = event["metadata"]
    repeat_hot_render = int(event.get("repeat_hot_render", 5))
    if repeat_hot_render < 1:
        repeat_hot_render = 1

    client = _s3_client(event)
    if client is None or not bucket or not object_key:
        return {
            "error": "missing_params_or_storage_config",
            "hint": "Need endpoint_url, keys, bucket+object_key (from A)",
        }

    cache_dir = _STABLE_PT_CACHE
    _reset_pt_cache_dir(cache_dir)
    zip_path = os.path.join(cache_dir, "_ow_chameleon_bundle.zip")

    try:
        _b_phase_s3_get_to_file(client, bucket, object_key, zip_path, latencies)
        _b_phase_unzip_to_cache(zip_path, cache_dir, latencies)
        _b_phase_remove_zip(zip_path)

        tmpl = _b_phase_page_template(cache_dir, latencies)
        options = _b_phase_build_options(num_of_rows, num_of_cols, latencies)
        _b_phase_first_render(tmpl, options, latencies)
        _b_phase_hot_renders(tmpl, options, repeat_hot_render, latencies)

        latencies["estimated_compile_overhead"] = max(
            0.0, latencies["first_render"] - latencies["hot_render_avg"]
        )
        latencies["function_execution"] = sum(
            [
                latencies["s3_download"],
                latencies["unzip_cache"],
                latencies["template_object_create"],
                latencies["input_build"],
                latencies["first_render"],
                latencies["hot_render_total"],
            ]
        )

        timestamps["finishing_time"] = time()
        return {
            "latencies": latencies,
            "timestamps": timestamps,
            "metadata": metadata,
            "cache_source": {"bucket": bucket, "object_key": object_key},
        }
    except Exception as e:
        return {"error": "chameleon_B_failed", "exception": repr(e)}


def main(event):
    event = dict(event or {})
    if bool(event.get("scalene") or event.get("profile_with_scalene")):
        return _profile_with_scalene_b(os.path.abspath(__file__), event)
    return _run_core(event)
