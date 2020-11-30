BINARY := metal-image-cache-sync
MAINMODULE := github.com/metal-stack/metal-image-cache-sync
COMMONDIR := $(or ${COMMONDIR},../builder)

include $(COMMONDIR)/Makefile.inc

release:: spec all;
