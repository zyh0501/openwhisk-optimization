import json
import os
import subprocess
import sys
import tempfile
from time import time

import six
from chameleon import PageTemplate


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


def _run_core(event):
    latencies = {}
    timestamps = {}
    timestamps["starting_time"] = time()
    num_of_rows = event["num_of_rows"]
    num_of_cols = event["num_of_cols"]
    metadata = event["metadata"]

    start = time()
    tmpl = PageTemplate(BIGTABLE_ZPT)

    data = {}
    for i in range(num_of_cols):
        data[str(i)] = i

    table = [data for x in range(num_of_rows)]
    options = {"table": table}

    data = tmpl.render(options=options)
    latency = time() - start
    latencies["function_execution"] = latency
    timestamps["finishing_time"] = time()

    return {"latencies": latencies, "timestamps": timestamps, "metadata": metadata}


def _profile_with_scalene(action_path: str, event: dict):
    """
    Profile only `_run_core(event)` via `python -m scalene ...`.

    Important: do NOT execute `function.py`'s `__main__` in this path, because
    OpenWhisk action runtimes may feed input differently.
    """
    outfile = os.path.join(tempfile.gettempdir(), "scalene-chameleon.json")
    return_full_profile = bool(event.get("return_full_scalene_profile", False))
    log_scalene_view = bool(event.get("log_scalene_view", True))

    # Persist the input for the profiled child process.
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
        encoding="utf-8",
    ) as ef:
        json.dump(event, ef)
        event_path = ef.name

    core_result_path = None

    # Create a tiny script that imports this file as a module and calls _run_core.
    # This avoids any reliance on __main__ behavior.
    profile_script = None
    child_env = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix="_core_result.json",
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
            script = (
                "import json\n"
                "import six\n"
                "from time import time\n"
                "from chameleon import PageTemplate\n"
                "BIGTABLE_ZPT = '''\\\n"
                "<table xmlns=\"http://www.w3.org/1999/xhtml\"\n"
                "xmlns:tal=\"http://xml.zope.org/namespaces/tal\">\n"
                "<tr tal:repeat=\"row python: options['table']\">\n"
                "<td tal:repeat=\"c python: row.values()\">\n"
                "<span tal:define=\"d python: c + 1\"\n"
                "tal:attributes=\"class python: 'column-' + %s(d)\"\n"
                "tal:content=\"python: d\" />\n"
                "</td>\n"
                "</tr>\n"
                "</table>''' % six.text_type.__name__\n"
                f"with open({event_path!r}, encoding='utf-8') as f: ev = json.load(f)\n"
                "try:\n"
                "    num_of_rows = int(ev['num_of_rows'])\n"
                "    num_of_cols = int(ev['num_of_cols'])\n"
                "    repeat_hot_render = int(ev.get('repeat_hot_render', 1))\n"
                "    if repeat_hot_render < 1:\n"
                "        repeat_hot_render = 1\n"
                "    latencies = {}\n"
                "    t0 = time()\n"
                "    tmpl = PageTemplate(BIGTABLE_ZPT)\n"
                "    latencies['template_object_create'] = time() - t0\n"
                "    t1 = time()\n"
                "    data = {}\n"
                "    for i in range(num_of_cols):\n"
                "        data[str(i)] = i\n"
                "    table = [data for _ in range(num_of_rows)]\n"
                "    options = {'table': table}\n"
                "    latencies['input_build'] = time() - t1\n"
                "    t2 = time()\n"
                "    _ = tmpl.render(options=options)\n"
                "    latencies['first_render'] = time() - t2\n"
                "    t3 = time()\n"
                "    for _ in range(repeat_hot_render):\n"
                "        _ = tmpl.render(options=options)\n"
                "    latencies['hot_render_total'] = time() - t3\n"
                "    latencies['hot_render_avg'] = latencies['hot_render_total'] / repeat_hot_render\n"
                "    latencies['estimated_compile_overhead'] = max(0.0, latencies['first_render'] - latencies['hot_render_avg'])\n"
                "    res = {'latencies': latencies}\n"
                "    payload = {\"ok\": True, \"result\": res}\n"
                "except Exception as e:\n"
                "    payload = {\"ok\": False, \"error\": repr(e)}\n"
                f"with open({core_result_path!r}, 'w', encoding='utf-8') as out_f:\n"
                "    json.dump(payload, out_f)\n"
            )
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
        # OpenWhisk action environments may sanitize environment variables.
        # Scalene requires PATH for redirecting the python executable.
        child_env.setdefault(
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        )

        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=600,
            env=child_env,
        )

        out = {
            "scalene_stderr": proc.stderr or "",
            "scalene_stdout_preview": (proc.stdout or "")[:4000],
            "scalene_returncode": proc.returncode,
        }

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
                            # Aggregate Scalene CPU percentages by our logical phases.
                            try:
                                files = profile_obj.get("files", {})
                                target = files.get(profile_obj.get("program"), {})
                                lines = target.get("lines", [])
                                ranges = {
                                    "template_object_create": range(20, 23),
                                    "input_build": range(23, 30),
                                    "first_render": range(30, 33),
                                    "hot_render": range(33, 36),
                                }
                                phase_pct = {}
                                for phase, linenos in ranges.items():
                                    total_pct = 0.0
                                    for ln in lines:
                                        if ln.get("lineno") in linenos:
                                            total_pct += float(ln.get("n_cpu_percent_python", 0.0))
                                            total_pct += float(ln.get("n_cpu_percent_c", 0.0))
                                            total_pct += float(ln.get("n_sys_percent", 0.0))
                                    phase_pct[phase] = total_pct
                                out["scalene_phase_cpu_percent"] = phase_pct
                            except Exception:
                                pass
                        except Exception:
                            out["scalene_profile_json_preview"] = raw[:2000]
            else:
                out["scalene_profile_json_present"] = False
        except OSError:
            out["scalene_profile_json_present"] = False

        # Print a CLI-friendly view into activation logs (stderr),
        # similar to float_operation/function1.py.
        if log_scalene_view and os.path.isfile(outfile):
            try:
                view_cmd = [
                    sys.executable,
                    "-m",
                    "scalene",
                    "view",
                    "--cli",
                    outfile,
                ]
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
                    print("=== Scalene view (--cli, truncated) ===", file=sys.stderr)
                    print(view_out[:8000], file=sys.stderr)
            except Exception:
                # Don't break action response JSON just because view failed.
                pass

        # Fallback: if Scalene didn't initialize / didn't write the profile,
        # still return the core execution result for usability.
        if (not out.get("core_result")) and (not out.get("scalene_profile_json_present")):
            out["core_result"] = {"ok": True, "result": _run_core(event)}

        return out
    except Exception as e:
        return {
            "error": "scalene_wrapper_exception",
            "exception": repr(e),
        }
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

    action_path = os.path.abspath(__file__)
    return _profile_with_scalene(action_path, event)


if __name__ == "__main__":
    # Some OpenWhisk python runtimes execute the action as a script and feed
    # the event JSON via stdin. Keep this block to ensure we always emit
    # valid JSON on stdout.
    try:
        if not sys.stdin.isatty():
            _event = json.load(sys.stdin)
        else:
            _event = {}
        result = main(_event)
    except Exception as e:
        result = {
            "error": "action_exception",
            "exception": repr(e),
        }
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")
