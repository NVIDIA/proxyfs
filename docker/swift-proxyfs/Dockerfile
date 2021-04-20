ARG DOCKER_SWIFT_TAG=latest

FROM dockerswiftaio/docker-swift:${DOCKER_SWIFT_TAG}

ARG PROXYFS_TAG
ARG GOLANG_VERSION=1.15.5

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
        build-essential \
        fuse \
        git \
        vim \
        wget \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install \
        requests \
        tox==3.5.3

COPY ./docker/swift-proxyfs/swift /etc/swift
RUN chown -R swift:swift /etc/swift

# Install Golang
ENV GOLANG_TARFILE_NAME=go${GOLANG_VERSION}.linux-amd64.tar.gz
RUN cd /tmp && wget -q https://dl.google.com/go/${GOLANG_TARFILE_NAME} && \
    tar -C /usr/local -xf /tmp/${GOLANG_TARFILE_NAME} && \
    rm -rf /tmp/${GOLANG_TARFILE_NAME}
ENV PATH=$PATH:/usr/local/go/bin

# Setup ProxyFS build environment
ENV GOPATH=/gopathroot
ENV PATH=$PATH:$GOPATH/bin
RUN echo "user_allow_other" >> /etc/fuse.conf

# Set up ProxyFS log file
# Create the directory where we'll place the ProxyFS code
# Create Fuse mount point directory
RUN mkdir -p /var/log/proxyfsd $GOPATH/src/github.com/NVIDIA/proxyfs /CommonMountPoint

WORKDIR $GOPATH/src/github.com/NVIDIA/proxyfs

# Checkout ProxyFS code
COPY .. $GOPATH/src/github.com/NVIDIA/proxyfs
RUN if [[ -n "$PROXYFS_TAG" ]] ; then git checkout $PROXYFS_TAG ; else echo "No ProxyFS tag was specified" ; fi
#RUN git clone https://github.com/NVIDIA/proxyfs.git $GOPATH/src/github.com/NVIDIA/proxyfs/
#RUN git checkout $PROXYFS_TAG

#RUN pip3 install -r $GOPATH/src/github.com/NVIDIA/proxyfs/pfs-swift-load/requirements.txt
#RUN install -m 0755 $GOPATH/src/github.com/NVIDIA/proxyfs/pfs-swift-load/pfs-swift-load-plot $GOPATH/bin/
RUN ln -s $GOPATH/src/github.com/NVIDIA/proxyfs/proxyfsd /etc/proxyfsd
RUN install -m 0755 $GOPATH/src/github.com/NVIDIA/proxyfs/bin/pfs_stat /usr/bin
RUN echo "/CommonMountPoint 127.0.0.1(rw,sync,fsid=1000,no_subtree_check,no_root_squash)" >> /etc/exports
RUN chmod +x /bin/fusermount
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN cd pfs_middleware  && python3 setup.py develop
RUN cd meta_middleware && python3 setup.py develop

RUN make clean minimal
RUN ln -s $GOPATH/bin/proxyfsd /usr/bin/proxyfsd

EXPOSE 15346
