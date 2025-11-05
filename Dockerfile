FROM golang:1.24-bookworm AS builder
WORKDIR /work
COPY . .
RUN make all

FROM debian:bookworm-slim AS certs
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY ca.pem /usr/local/share/ca-certificates/custom-ca.crt
RUN update-ca-certificates

FROM gcr.io/distroless/static-debian12
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /work/bin/metal-image-cache-sync /metal-image-cache-sync
CMD ["/metal-image-cache-sync"]
