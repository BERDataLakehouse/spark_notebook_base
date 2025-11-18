# BERDL Jupyter Base Image

Base image with PySpark, Java dependencies, and system utilities for BER Data Lakehouse.

## Build the Docker images

Using docker:
```sh
$ docker build . -t <image_name>:<image_tag_or_version>
```

Using podman:
```sh
$ podman build . -f Dockerfile --platform=linux/amd64 --format docker
```

## When to Modify

### Base Image

Python libraries (managed with uv), JARs, system executables

### Child Images

BERDL-specific configurations and files, entrypoint scripts and startup hooks go into the
child image repository


## Python Dependencies

The Python packages installed on the spark notebook base docker images are bundled in the [pyproject.toml](pyproject.toml) file for ease of installation and management using the [package management tool uv](https://docs.astral.sh/uv/).

Downstream python packages installed on the `spark_notebook_base` image or its derivatives can add these dependencies using the command:

```sh
$ uv add git+https://github.com/BERDataLakehouse/spark_notebook_base.git
```

This will import the package `berdl-notebook-python-base` into the project dependencies; `berdl-notebook-python-base` provides no code of its own, it just installs the dependencies listed in the `pyproject.toml`.

### Bring your local venv into sync with the pyproject.toml file

```sh
$ uv sync
```

See the [uv docs](https://docs.astral.sh/uv/reference/cli/) for more details.


### Install these dependencies in another project

```sh
$ uv add git+https://github.com/BERDataLakehouse/spark_notebook_base.git
```

### Install development dependencies

#### Local install

```sh
$ uv sync --group dev
```

#### On the spark_notebook_base docker image

Packages are installed into the system python path.

```sh
$ uv pip install --system --group dev
```
