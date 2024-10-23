FROM ghcr.io/nilsnolde/docker-valhalla/valhalla:latest

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
WORKDIR /custom_files
USER root

# Install Python package dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libudunits2-dev proj-bin gdal-bin libgeos-dev \
    python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies for opentimes work inside docker
COPY ./pyproject.toml .
RUN pip install --no-cache-dir --break-system-packages .[data]
