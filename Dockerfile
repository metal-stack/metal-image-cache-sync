FROM golang:1.24-bookworm AS builder

WORKDIR /work
COPY . .
RUN make all

FROM gcr.io/distroless/static-debian12
COPY --from=builder /work/bin/metal-image-cache-sync /metal-image-cache-sync
CMD ["/metal-image-cache-sync"]
