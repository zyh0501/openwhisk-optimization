from time import time
import six
import json
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


def main(event):
    latencies = {}
    timestamps = {}
    timestamps["starting_time"] = time()
    
    # 获取参数
    num_of_rows = event['num_of_rows']
    num_of_cols = event['num_of_cols']
    metadata = event['metadata']
    # 热渲染重复次数，默认为5
    repeat_hot_render = 5

    # ========== 阶段1：模板编译 ==========
    t0 = time()
    tmpl = PageTemplate(BIGTABLE_ZPT)
    latencies['template_object_create'] = time() - t0

    # ========== 阶段2：数据准备 ==========
    t1 = time()
    data = {}
    for i in range(num_of_cols):
        data[str(i)] = i
    table = [data for x in range(num_of_rows)]
    options = {'table': table}
    latencies['input_build'] = time() - t1

    # ========== 阶段3：首次渲染（包含编译缓存影响） ==========
    t2 = time()
    result = tmpl.render(options=options)
    latencies['first_render'] = time() - t2

    # ========== 阶段4：热渲染（重复渲染） ==========
    t3 = time()
    for _ in range(repeat_hot_render):
        _ = tmpl.render(options=options)
    latencies['hot_render_total'] = time() - t3
    latencies['hot_render_avg'] = latencies['hot_render_total'] / repeat_hot_render

    # ========== 阶段5：估算编译开销 ==========
    # 编译开销 = 首次渲染 - 热渲染平均
    latencies['estimated_compile_overhead'] = max(0.0, 
        latencies['first_render'] - latencies['hot_render_avg']
    )
    
    # 总执行时间（保持向后兼容）
    latencies['function_execution'] = (
        latencies['template_object_create'] + 
        latencies['input_build'] + 
        latencies['first_render'] + 
        latencies['hot_render_total']
    )
    
    timestamps["finishing_time"] = time()

    return {
        "latencies": latencies, "timestamps": timestamps, "metadata": metadata}