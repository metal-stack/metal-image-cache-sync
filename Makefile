BINARY := metal-image-cache-sync
MAINMODULE := github.com/metal-stack/metal-image-cache-sync/cmd

SHA := $(shell git rev-parse --short=8 HEAD)
GITVERSION := $(shell git describe --long --all)
BUILDDATE := $(shell date -Iseconds)
VERSION := $(or ${VERSION},$(shell git describe --tags --exact-match 2> /dev/null || git symbolic-ref -q --short HEAD || git rev-parse --short HEAD))


# default points to mini-lab
METAL_API_ENDPOINT := $(or ${METALCTL_API_URL},http://api.0.0.0.0.nip.io:8080/metal)
METAL_API_HMAC := $(or ${METALCTL_HMAC},metal-view)

LINKMODE := -extldflags '-static -s -w'

all: test build

.PHONY: build
build:
	go build -tags netgo,osusergo,urfave_cli_no_docs \
		 -ldflags "$(LINKMODE) -X 'github.com/metal-stack/v.Version=$(VERSION)' \
								   -X 'github.com/metal-stack/v.Revision=$(GITVERSION)' \
								   -X 'github.com/metal-stack/v.GitSHA1=$(SHA)' \
								   -X 'github.com/metal-stack/v.BuildDate=$(BUILDDATE)'" \
	   -o bin/$(BINARY) $(MAINMODULE)
	strip bin/$(BINARY)

.PHONY: test
test:
	go test ./... -race -coverprofile=coverage.out -covermode=atomic && go tool cover -func=coverage.out

.PHONY: start
start: all
	mkdir -p /tmp/metal-image-cache
	bin/$(BINARY) \
	  --log-level debug \
	  --metal-api-endpoint $(METAL_API_ENDPOINT) \
	  --metal-api-hmac $(METAL_API_HMAC) \
	  --max-cache-size 10G \
	  --min-images-per-name 2 \
	  --cache-root-path /tmp/metal-image-cache \
	  --enable-kernel-cache \
	  --enable-boot-image-cache \
	#   --dry-run
