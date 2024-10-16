FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
WORKDIR /times

# Install deps necessary for further installs
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget libcurl4-openssl-dev ca-certificates

# Add the R and Eclipse apt repositories
RUN echo 'deb https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/' \
    >> /etc/apt/sources.list && \
    echo "deb https://packages.adoptium.net/artifactory/deb jammy main" \
    >> /etc/apt/sources.list

# Fetch the R public key: https://cran.r-project.org/bin/linux/ubuntu/fullREADME.html
RUN wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc \
    | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc && \
    wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public \
    | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc

# Install all R and Python package dependencies and JDK 21
RUN apt-get update && \
    apt-get install -y --no-install-recommends libudunits2-dev=2.2.28-3 \
    proj-bin=8.2.1-1 gdal-bin=3.4.1+dfsg-1build4 \
    libgeos-dev=3.10.2-1 libgit2-1.1=1.1.0+dfsg.1-4.1build1 \
    python3-pip=22.0.2+dfsg-1ubuntu0.4 \
    r-base-core=4.4.1-3.2204.0 r-base-dev=4.4.1-3.2204.0 \
    libssl-dev=3.0.2-0ubuntu1.18 libfontconfig1-dev=2.13.1-4.2ubuntu5 \
    libxml2-dev=2.9.13+dfsg-1ubuntu0.4 libharfbuzz-dev=2.7.4-1ubuntu3.1 \
    libfribidi-dev=1.0.8-2ubuntu3.1 libfreetype6-dev=2.11.1+dfsg-1ubuntu0.2 \
    libpng-dev=1.6.37-3build5 libtiff5-dev=4.3.0-6 \
    libjpeg-dev=8c-2ubuntu10 \
    temurin-21-jdk && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY ./pyproject.toml .
RUN pip install --no-cache-dir .[data]

# Install R dependencies
COPY ./data/renv/activate.R ./renv/activate.R
COPY ./data/renv/settings.json ./renv/settings.json
COPY ./data/renv.lock ./data/.Rprofile ./data/.renvignore ./data/.Renviron ./
RUN R CMD javareconf && R -e "renv::restore()"
