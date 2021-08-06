# To build this image:
#
#   docker build                                                    \
#          [--target {development|production}]                      \
#          [--build-arg GolangVersion=<X.YY.Z>]                     \
#          [--build-arg ProxyFSVersion=<branch-name-or-tag-or-SHA>] \
#          [--build-arg MakeTarget={|all|ci|minimal}]               \
#          [--no-cache]                                             \
#          [=t <repository>[:<tag>]]                                .
#
# To run the resultant image:
#
#   docker run                                            \
#          --rm                                           \
#          -it                                            \
#          [--mount src="$(pwd)",target="/src",type=bind] \
#          <image id>|<repository>[:<tag>]

FROM alpine:3.14.0 as base
ARG GolangVersion=1.16.7
ARG ProxyFSVersion
ARG MakeTarget
RUN apk add --no-cache libc6-compat

FROM base as development
RUN apk add --no-cache git
RUN apk add --no-cache gcc
RUN apk add --no-cache libc-dev
RUN apk add --no-cache make
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
RUN git clone https://github.com/NVIDIA/proxyfs.git /src
WORKDIR /src
RUN git checkout $ProxyFSVersion
RUN make $MakeTarget

FROM base as production
COPY --from=pre-production /src/imgr/imgr ./
COPY --from=pre-production /src/imgr/imgr.conf ./
CMD ["./imgr", "imgr.conf"]
