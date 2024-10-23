FROM ghcr.io/nilsnolde/docker-valhalla/valhalla:latest

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
WORKDIR /custom_files
USER root

# Install Python package dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    osmium-tool=1.16.0-1build1 libzstd1=1.5.5+dfsg2-2build1.1 \
    libudunits2-dev=2.2.28-7build1 libproj-dev=9.4.0-1build2 \
    gdal-bin=3.8.4+dfsg-3ubuntu3 geos-bin=3.12.1-3build1 \
    python3-pip=24.0+dfsg-1ubuntu1 && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies for opentimes work inside docker
COPY ./pyproject.toml .
RUN pip install --no-cache-dir --break-system-packages .[data]
USER valhalla
