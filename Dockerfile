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
#       1) a make clean is performed to clean out context dir copy
#       2) MakeTarget defaults to blank (equivalent to "all")
#       3) ensure MakeTarget actually builds imgr
#       4) this is the default image
#       5) image will likely need to customize imgr.conf
#     --no-cache:
#       1) tells Docker to ignore cached images that might be stale
#       2) useful due to Docker not understanding changes to build-args
#       3) useful due to Docker not understanding changes to context dir
#
# To run the resultant image:
#
#   docker run                                            \
#          [-it]                                          \
#          [--rm]                                         \
#          [--mount src="$(pwd)",target="/src",type=bind] \
#          <image id>|<repository>[:<tag>]
#
#   Notes:
#     -it:  tells Docker to run container interactively
#     --rm: tells Docker to destroy container upon exit
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
RUN apk add --no-cache curl
RUN apk add --no-cache gcc
RUN apk add --no-cache git
RUN apk add --no-cache jq
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
COPY . /src
WORKDIR /src
RUN make clean
RUN make $MakeTarget

FROM base as production
COPY --from=pre-production /src/imgr/imgr ./
COPY --from=pre-production /src/imgr/imgr.conf ./
CMD ["./imgr", "imgr.conf"]
