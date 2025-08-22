# Development Environment Setup
* uv init
* uv pip install -r dev/requirements-dev.txt

# Build the Docker images 
* podman build . -f Dockerfile --platform=linux/amd64 --format docker
* podman build . -f Dockerfile --platform=linux/amd64 --format docker