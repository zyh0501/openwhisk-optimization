FROM openwhisk/action-python-v3.9:1.20.0

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade "pip>=21" setuptools wheel \
    && pip3 install --no-cache-dir -r /tmp/requirements.txt
