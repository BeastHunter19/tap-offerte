FROM apache/spark:3.4.4-python3

USER root

# Create directory for spark libs cache volume with correct ownership
ARG SPARK_USER_UID=185
ARG SPARK_USER_GID=185

RUN mkdir -p /opt/spark-ivy-cache && \
    chown -R ${SPARK_USER_UID}:${SPARK_USER_GID} /opt/spark-ivy-cache

# Install Python 3.9 and dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.9 python3.9-distutils python3.9-venv curl && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1 && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9

# Ensure pip points to Python 3.9's pip
RUN ln -sf /usr/bin/python3.9 /usr/bin/python && \
    ln -sf /usr/local/bin/pip /usr/bin/pip

# Reinstall required Python packages using Python 3.9
RUN pip install --upgrade pip && \
    pip install google-genai elasticsearch pandas pyarrow

USER spark