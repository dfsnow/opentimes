# From: https://github.com/valhalla/valhalla/blob/master/Dockerfile
# This builds the actual Valhalla binaries. The only change here from the base
# Valhalla Docker image is enabling DENABLE_THREAD_SAFE_TILE_REF_COUNT for
# a global tile cache in memory
FROM ubuntu:24.04 AS valhalla_builder

ARG CONCURRENCY
ARG ADDITIONAL_TARGETS

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH
ENV LD_LIBRARY_PATH=/usr/local/lib:/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu:/lib32:/usr/lib32
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt update && apt install -y sudo git

WORKDIR /usr/local/src
RUN git clone --depth 1 --branch 3.5.1 https://github.com/valhalla/valhalla && \
    bash /usr/local/src/valhalla/scripts/install-linux-deps.sh && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/src/valhalla
RUN git submodule sync && git submodule update --init --recursive && \
    rm -rf build && mkdir build

WORKDIR /usr/local/src/valhalla/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc \
    -DENABLE_SINGLE_FILES_WERROR=Off -DENABLE_THREAD_SAFE_TILE_REF_COUNT=On \
    -DENABLE_PYTHON_BINDINGS=Off -DENABLE_TESTS=Off -DENABLE_BENCHMARKS=Off && \
    make all ${ADDITIONAL_TARGETS} -j${CONCURRENCY:-$(nproc)} && \
    make install

WORKDIR /usr/local/src
RUN cd valhalla && echo "https://github.com/valhalla/valhalla/tree/$(git rev-parse HEAD)" > ../valhalla_version
RUN for f in valhalla/locales/*.json; do cat ${f} | python3 -c 'import sys; import json; print(json.load(sys.stdin)["posix_locale"])'; done > valhalla_locales
RUN rm -rf valhalla

FROM ubuntu:24.04 AS valhalla_builder2

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH
ENV LD_LIBRARY_PATH=/usr/local/lib:/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu:/lib32:/usr/lib32
COPY --from=valhalla_builder /usr/local /usr/local

RUN export DEBIAN_FRONTEND=noninteractive && apt update && \
    apt install -y libcurl4 libczmq4 libluajit-5.1-2 libgdal34 \
    libprotobuf-lite32 libsqlite3-0 libsqlite3-mod-spatialite libzmq5 zlib1g \
    curl gdb locales parallel python3-minimal python-is-python3 python3-shapely python3-requests \
    spatialite-bin unzip wget && rm -rf /var/lib/apt/lists/*
RUN cat /usr/local/src/valhalla_locales | xargs -d '\n' -n1 locale-gen

# Now build the GIS-Ops image from:
# https://github.com/nilsnolde/docker-valhalla/blob/master/Dockerfile
RUN cd /usr/local/bin && \
  preserve="valhalla_service valhalla_build_tiles valhalla_build_config valhalla_build_admins valhalla_build_timezones valhalla_build_elevation valhalla_ways_to_edges valhalla_build_extract valhalla_export_edges valhalla_add_predicted_traffic valhalla_ingest_transit valhalla_convert_transit valhalla_add_landmarks valhalla_build_landmarks" && \
  mv $preserve .. && \
  for f in valhalla*; do rm $f; done && \
  cd .. && mv $preserve ./bin

FROM ubuntu:24.04 AS valhalla_runner

RUN apt-get update > /dev/null && \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y libluajit-5.1-2 libgdal34 git \
    libzmq5 libczmq4 spatialite-bin libprotobuf-lite32 sudo locales \
    libsqlite3-0 libsqlite3-mod-spatialite libcurl4 \
    python3.12-minimal python3-requests python3-shapely python-is-python3 \
    curl unzip moreutils jq spatialite-bin > /dev/null

COPY --from=valhalla_builder2 /usr/local /usr/local

ENV use_tiles_ignore_pbf=True
ENV build_tar=True
ENV serve_tiles=True
ENV update_existing_config=True
ENV default_speeds_config_url="https://raw.githubusercontent.com/OpenStreetMapSpeeds/schema/master/default_speeds.json"

# Create a new valhalla user with correct ids
# https://jtreminio.com/blog/running-docker-containers-as-current-host-user
ARG USER_ID=59999
ARG GROUP_ID=59999
RUN if getent group valhalla; then groupdel valhalla; fi && \
    if getent group ${GROUP_ID}; then groupdel $(getent group ${GROUP_ID} | cut -d: -f1); fi && \
    groupadd -g ${GROUP_ID} valhalla && \
    useradd -l -u ${USER_ID} -g valhalla valhalla && \
    install -d -m 0755 -o valhalla -g valhalla /home/valhalla && \
    rm -rf /custom_files && mkdir -p /custom_files && \
    chmod 0775 /custom_files && \
    chown valhalla:valhalla /custom_files

WORKDIR /tmp
RUN git clone --depth 1 https://github.com/nilsnolde/docker-valhalla && \
    mkdir -p /valhalla/scripts && \
    cp -r ./docker-valhalla/scripts/* /valhalla/scripts

USER valhalla
WORKDIR /custom_files
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH
ENV LD_LIBRARY_PATH=/usr/local/lib:/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu:/lib32:/usr/lib32

EXPOSE 8002
ENTRYPOINT ["/valhalla/scripts/run.sh"]
CMD ["build_tiles"]
