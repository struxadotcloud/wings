FROM alpine:latest

RUN apk add --no-cache ca-certificates coreutils curl btrfs-progs xfsprogs-extra zfs restic && \
	update-ca-certificates

# Add struxa-wings and entrypoint
ARG TARGETPLATFORM
COPY .docker/${TARGETPLATFORM#linux/}/struxa-wings /usr/bin/struxa-wings

ENV OCI_CONTAINER=official

ENTRYPOINT ["/usr/bin/struxa-wings"]
