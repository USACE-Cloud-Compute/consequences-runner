FROM ghcr.io/osgeo/gdal:ubuntu-full-3.8.3 AS dev

# ARG TILEDB_LIB=/usr/local/lib/tiledb
ARG GO_VERSION=1.24.5
ARG TARGETARCH

ENV PATH=/go/bin:$PATH
# ENV CGO_LDFLAGS="-L${TILEDB_LIB}/lib"
# ENV CGO_CFLAGS="-I${TILEDB_LIB}/include"
ENV GOROOT=/go
ENV GOPATH=/src/go

RUN echo "Building for arch: ${TARGETARCH}" &&\
    wget https://golang.org/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz -P / &&\
    tar -xvzf /go${GO_VERSION}.linux-${TARGETARCH}.tar.gz -C / 
    
RUN apt-get update && apt-get install -y \
    gdal-data \
    gdal-bin \
    libgdal-dev\
    git \
&& rm -rf /var/lib/apt/lists/*
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