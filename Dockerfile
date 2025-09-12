FROM gradle:9.0.0-jdk24-ubi-minimal AS builder
# Setup Java dependencies. Cut a release to build this image on GHA
WORKDIR /build
COPY build.gradle.kts .
RUN gradle copyLibs --no-daemon
RUN gradle dependencies --configuration runtimeClasspath > /build/libs/dependencies.txt

# This is a 4.0 tag from August 2025
FROM quay.io/jupyter/pyspark-notebook@sha256:938fdd57901e764b4e6e0adbe7438725e703a782608dc79bd2a7c722a4f8a0bf

USER root
ENV MC_VER=2025-07-21T05-28-08Z
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

RUN unset HOME
WORKDIR /deps
COPY requirements.txt /deps/
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_CONF_DIR=${SPARK_HOME}/conf
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.9-src.zip:/opt/conda/:${PYTHONPATH}
RUN eval "$(conda shell.bash hook)" && /opt/conda/bin/pip install uv==0.8.17
RUN eval "$(conda shell.bash hook)" && uv pip install --system -r requirements.txt
RUN rm -rf /home/jovyan/
