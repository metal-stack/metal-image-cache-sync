BINARY := metal-image-cache-sync
MAINMODULE := github.com/metal-stack/metal-image-cache-sync/cmd
COMMONDIR := $(or ${COMMONDIR},../builder)

# default points to mini-lab
METAL_API_ENDPOINT := $(or ${METALCTL_URL},http://api.0.0.0.0.xip.io:8080/metal)
METAL_API_HMAC := $(or ${METALCTL_HMAC},metal-view)

include $(COMMONDIR)/Makefile.inc

.PHONY: all
all::
	go mod tidy

release:: all;

.PHONY: start
start: all
	mkdir -p /tmp/metal-image-cache
	bin/metal-image-cache-sync \
	  --log-level debug \
	  --metal-api-endpoint $(METAL_API_ENDPOINT) \
	  --metal-api-hmac $(METAL_API_HMAC) \
	  --max-cache-size 10G \
	  --min-images-per-name 2 \
	  --cache-root-path /tmp/metal-image-cache \
	  --enable-kernel-cache \
	  --enable-boot-image-cache \
	#   --dry-run
