# 基于 OpenWhisk Python 运行时
FROM openwhisk/action-python-v3.9

# 安装 scalene 及其依赖
RUN pip install --no-cache-dir scalene

# 验证安装
RUN python -c "import scalene; print('Scalene installed successfully')"

# 设置工作目录
WORKDIR /pythonAction

# 复制你的函数文件（可选，也可以在创建 action 时指定）
# COPY function.py /pythonAction/__main__.py