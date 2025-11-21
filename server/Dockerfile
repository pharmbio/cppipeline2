FROM ubuntu:22.04

# update and install
RUN apt update && \
    apt install -y python3 python3-pip vim openssh-client rsync ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# install python packages via requirements.txt (better caching)
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --upgrade pip && \
    PIP_NO_CACHE_DIR=1 pip3 install -r /tmp/requirements.txt

# set timezone
ENV TZ=Europe/Stockholm
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# create non-root user with uid 20000 for correct file ownership on host mounts
RUN groupadd -g 20000 cpp && \
    useradd -m -u 20000 -g 20000 -s /bin/bash cpp && \
    mkdir -p /home/cpp/.ssh && \
    chown -R 20000:20000 /home/cpp/.ssh && \
    chmod 700 /home/cpp/.ssh

# ensure default work dirs exist (useful if volumes not mounted, and for permissions)
RUN mkdir -p /cpp_work/logs && chown -R cpp:cpp /cpp_work

# default user and workdir
ENV HOME=/home/cpp
USER cpp
WORKDIR /cpp

# bake application source into the image
COPY --chown=cpp:cpp . /cpp
