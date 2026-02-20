FROM gradle:9.1.0-jdk24-ubi-minimal AS builder
# Build Java dependencies (JARs are copied to Spark jars directory below)
WORKDIR /build
COPY build.gradle.kts .
COPY src/ src/
RUN gradle copyLibs --no-daemon
RUN gradle dependencies --configuration runtimeClasspath > /build/libs/dependencies.txt

# This is a spark-4.0.1 tag from November 2025
FROM quay.io/jupyter/pyspark-notebook@sha256:6287c0ba787930d8dec08f8c5c81866b1e86be14adbfb3efe2b18d4e5db877ff

USER root
ENV MC_VER=2025-08-13T08-35-41Z
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
# Install Node.js, gh CLI, and AI coding assistants (claude, codex)
# HOME=/root required because ENV HOME= is set above for the build context
RUN eval "$(conda shell.bash hook)" && conda install -y -c conda-forge nodejs gh
RUN HOME=/root npm install -g @anthropic-ai/claude-code @openai/codex
# Allow users to install npm packages to their home dir (PVC-backed, survives restarts)
ENV NPM_CONFIG_PREFIX=/home/jovyan/.npm-global
ENV PATH=/home/jovyan/.npm-global/bin:$PATH
RUN eval "$(conda shell.bash hook)" && uv pip install --system /deps/ && rm -rf /deps
RUN rm -rf /home/jovyan/
