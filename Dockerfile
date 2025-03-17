FROM golang:1.24-alpine AS builder

WORKDIR /work
COPY . .
RUN apk add make \
 && make all

FROM gcr.io/distroless/static-debian12
COPY --from=builder /work/bin/metal-image-cache-sync /metal-image-cache-sync
CMD ["/metal-image-cache-sync"]
