FROM gradle:9.0.0-jdk24-ubi-minimal AS builder
# Setup Java dependencies. Cut a release to build this image on GHA
WORKDIR /build
COPY build.gradle.kts .
RUN gradle copyLibs --no-daemon

FROM quay.io/jupyter/pyspark-notebook:spark-4.0.0
USER root
COPY --from=builder /build/libs/ /usr/local/spark/jars/


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

# Remove conflicting Hive 2.3.10 jars
RUN rm -f /usr/local/spark/jars/hive-*-2.3.10.jar /usr/local/spark/jars/hive-shims-*.jar /usr/local/spark/jars/spark-hive*.jar




# Update python and install dependencies
WORKDIR /deps
COPY pyproject.toml uv.lock .python-version /deps/

# See https://github.com/astral-sh/uv/issues/11315
RUN pip install uv
ENV VIRTUAL_ENV=/opt/conda
RUN uv sync --locked --inexact --no-dev
RUN rm -rf /home/jovyan/