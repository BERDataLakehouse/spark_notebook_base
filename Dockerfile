FROM gradle:9.1.0-jdk24-ubi-minimal AS builder
# Setup Java dependencies. Cut a release to build this image on GHA
WORKDIR /build
COPY build.gradle.kts .
RUN gradle copyLibs --no-daemon
RUN gradle dependencies --configuration runtimeClasspath > /build/libs/dependencies.txt

# This is a spark-4.0.1 tag from November 2025
FROM quay.io/jupyter/pyspark-notebook@sha256:0381a3db0df7cb563db35484a11d4373ac833a0c878b70797c3ca15d947b5bbb

USER root
ENV MC_VER=2016-02-19T04-11-55Z
RUN apt-get update && apt-get install -y --no-install-recommends \
        gettext \
        vim \
        redis-tools \
        wget \
    && wget -q https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.${MC_VER} -O /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc \
    && apt-get purge -y --auto-remove wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/libs/ /usr/local/spark/jars/

ENV HOME=
WORKDIR /deps
COPY pyproject.toml /deps/
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_CONF_DIR=${SPARK_HOME}/conf
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.9-src.zip:/opt/conda/:${PYTHONPATH}
RUN eval "$(conda shell.bash hook)" && /opt/conda/bin/pip install uv==0.8.17
# Install Node.js for building JupyterLab extensions
RUN eval "$(conda shell.bash hook)" && conda install -y -c conda-forge nodejs
RUN eval "$(conda shell.bash hook)" && uv pip install --system /deps/ && rm -rf /deps
RUN rm -rf /home/jovyan/
