# To build this image:
#
#   docker build                                      \
#          [--target {development|production}]        \
#          [--build-arg GolangVersion=<X.YY.Z>]       \
#          [--build-arg MakeTarget={|all|ci|minimal}] \
#          [--no-cache]                               \
#          [-t <repository>[:<tag>]]                  .
#
#   Notes:
#     --target development:
#       1) builds an image capable of building all elements
#       2) since the make isn't run, MakeTarget is ignored
#     --target production:
#       1) this is the default image
#       2) imgr image actually built in --target pre-production
#     --build-arg GolangVersion:
#       1) identifies Golang version
#       2) default specified in ARG GolangVersion line in --target base
#     --build-arg MakeTarget:
#       1) identifies Makefile target(s) to build (following make clean)
#       2) defaults to blank (equivalent to "all")
#       3) used in --target pre-production
#       4) hence applicable to --target production
#       4) ensure MakeTarget actually builds imgr
#     --no-cache:
#       1) tells Docker to ignore cached images that might be stale
#       2) useful due to Docker not understanding changes to build-args
#       3) useful due to Docker not understanding changes to context dir
#     -t:
#       1) provides a name REPOSITORY:TAG for the built image
#       2) if no tag is specified, TAG will be "latest"
#       3) if no repository is specified, only the IMAGE ID will identify the built image
#
# To run the resultant image:
#
#   docker run                                            \
#          [-d|--detach]                                  \
#          [-it]                                          \
#          [--rm]                                         \
#          [--mount src="$(pwd)",target="/src",type=bind] \
#          <image id>|<repository>[:<tag>]
#
#   Notes:
#     -d|--detach: tells Docker to detach from running container 
#     -it:         tells Docker to run container interactively
#     --rm:        tells Docker to destroy container upon exit
#     --mount:
#       1) bind mounts the context into /src in the container
#       2) /src will be a read-write'able equivalent to the context dir
#       3) only useful for --target development
#       4) /src will be a local copy instead for --target pre-production
#       5) /src doesn't exist for --target production

FROM alpine:3.14.0 as base
ARG GolangVersion=1.16.7
ARG MakeTarget
RUN apk add --no-cache libc6-compat

FROM base as development
RUN apk add --no-cache bind-tools
RUN apk add --no-cache curl
RUN apk add --no-cache gcc
RUN apk add --no-cache git
RUN apk add --no-cache jq
RUN apk add --no-cache libc-dev
RUN apk add --no-cache make
RUN apk add --no-cache tar
RUN curl -sSL https://github.com/coreos/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz \
    | tar -vxz -C /usr/local/bin --strip=1 etcd-v3.5.0-linux-amd64/etcd etcd-v3.5.0-linux-amd64/etcdctl \
    && chown root:root /usr/local/bin/etcd /usr/local/bin/etcdctl
ENV GolangBasename "go${GolangVersion}.linux-amd64.tar.gz"
ENV GolangURL      "https://golang.org/dl/${GolangBasename}"
WORKDIR /tmp
RUN wget -nv $GolangURL
RUN tar -C /usr/local -xzf $GolangBasename
ENV PATH $PATH:/usr/local/go/bin
VOLUME /src
WORKDIR /src

FROM development as pre-production
VOLUME /src
COPY . /src
WORKDIR /src
RUN make clean
RUN make $MakeTarget

FROM base as production
COPY --from=pre-production /src/icert/icert ./
RUN ./icert -ca -ed25519 -caCert caCert.pem -caKey caKey.pem -ttl 3560
RUN ./icert -ed25519 -caCert caCert.pem -caKey caKey.pem -ttl 3560 -cert cert.pem -key key.pem -dns production
COPY --from=pre-production /src/imgr/imgr ./
COPY --from=pre-production /src/imgr/production.conf ./
CMD ["./imgr", "production.conf"]
