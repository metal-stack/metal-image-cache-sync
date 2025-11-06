FROM golang:1.24-bookworm AS builder
WORKDIR /work
COPY . .
RUN make all

FROM debian:bookworm-slim AS certs
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY ca.pem /usr/local/share/ca-certificates/custom-ca.crt
RUN update-ca-certificates

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl bash netcat-traditional iputils-ping && \
    rm -rf /var/lib/apt/lists/*

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /work/bin/metal-image-cache-sync /metal-image-cache-sync

# Default shell for interactive execs
SHELL ["/bin/bash", "-c"]

ENTRYPOINT ["/metal-image-cache-sync"]
