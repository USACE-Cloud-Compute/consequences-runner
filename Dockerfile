FROM ghcr.io/osgeo/gdal:ubuntu-full-3.8.3 AS dev

ARG GO_VERSION=1.27.1
ARG TARGETARCH=amd64

ENV PATH=/go/bin:$PATH
ENV GOROOT=/go
ENV GOPATH=/src/go

# 1. Install Go securely
RUN echo "Building for arch: ${TARGETARCH}" && \
    wget https://golang.org/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz -P / && \
    tar -xvzf /go${GO_VERSION}.linux-${TARGETARCH}.tar.gz -C / && \
    rm /go${GO_VERSION}.linux-${TARGETARCH}.tar.gz


#------------

FROM dev AS builder

COPY . /src

WORKDIR /src

#RUN go build
RUN go build

#-------------

FROM ubuntu:24.04 AS prod

ARG TILEDB_LIB=/usr/local/lib/tiledb

ENV PATH=/root/.local/bin:$PATH
ENV LD_LIBRARY_PATH="${TILEDB_LIB}/lib"
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
ENV LIBRARY_PATH="${TILEDB_LIB}/lib"

RUN apt update &&\
    apt -y install libssl-dev libbz2-dev libgdbm-dev uuid-dev libncurses-dev libffi-dev libgdbm-compat-dev sqlite3 lzma lzma-dev &&\
    apt -y install gdal-bin gdal-data libgdal-dev

COPY --from=builder /usr/local/lib/tiledb /usr/local/lib/tiledb
COPY --from=builder /src/consequences-runner /app/consequences-runner