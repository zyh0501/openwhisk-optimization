FROM openwhisk/action-python-v3.9:latest

COPY requirements.txt requirements.txt
RUN python3 -m pip install --upgrade pip &&\
    python3 -m pip install --no-cache-dir -r requirements.txt &&\
    ln -sf /usr/local/bin/python3 /usr/local/bin/python

# This base image uses Debian buster (EOL). Switch to archive repos.
RUN sed -i 's|deb.debian.org/debian|archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list && \
    sed -i '/buster-updates/d' /etc/apt/sources.list && \
    printf 'Acquire::Check-Valid-Until "false";\n' > /etc/apt/apt.conf.d/99no-check-valid && \
    apt-get update && apt-get install -y --no-install-recommends \
    libsm6 \
    libxrender1 \
    libfontconfig1 \
    libxext6 \
    libxrender-dev \
    && rm -rf /var/lib/apt/lists/*