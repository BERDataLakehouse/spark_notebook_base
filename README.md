# BERDL Jupyter Base Image
Base image with PySpark, Java dependencies, and system utilities for BER Data Lakehouse.

## When to Modify

### Base Image:
Python libraries (managed with uv), JARs, system executables

### Child Images:
BERDL-specific configurations and files, entrypoint scripts and startup hooks go into the
child image repository

## Build the Docker images

Using docker:
* docker build . -t <image_name>:<image_tag_or_version>

Using podman:
* podman build . -f Dockerfile --platform=linux/amd64 --format docker
