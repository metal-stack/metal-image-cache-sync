FROM gcr.io/distroless/static-debian13
COPY bin/metal-image-cache-sync /metal-image-cache-sync
CMD ["/metal-image-cache-sync"]
