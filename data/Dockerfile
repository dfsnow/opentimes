FROM ghcr.io/nilsnolde/docker-valhalla/valhalla:latest
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
USER root

# Create a new valhalla user with correct ids
# https://jtreminio.com/blog/running-docker-containers-as-current-host-user
ARG USER_ID=59999
ARG GROUP_ID=59999
RUN userdel -f valhalla && \
    if getent group valhalla; then groupdel valhalla; fi && \
    if getent group ${GROUP_ID}; then groupdel $(getent group ${GROUP_ID} | cut -d: -f1); fi && \
    groupadd -g ${GROUP_ID} valhalla && \
    useradd -l -u ${USER_ID} -g valhalla valhalla && \
    install -d -m 0755 -o valhalla -g valhalla /home/valhalla && \
    rm -rf /custom_files && mkdir -p /custom_files && \
    chmod 0775 /custom_files && \
    chown valhalla:valhalla /custom_files

WORKDIR /custom_files
USER valhalla
