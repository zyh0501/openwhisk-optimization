FROM openwhisk/action-python-v3.9:latest

COPY ops.py ops.py
COPY setup.py setup.py
COPY requirements.txt requirements.txt
RUN python3 -m pip install --upgrade pip &&\
    python3 -m pip install --no-cache-dir -r requirements.txt &&\
    ln -sf /usr/local/bin/python3 /usr/local/bin/python
RUN python3 -m pip install -e .