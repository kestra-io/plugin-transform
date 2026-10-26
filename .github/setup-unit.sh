#!/bin/sh
set -e

# the Arrow tests run aq in the image pinned by the task, build it when it is not published yet
REVISION=$(sed -n 's/^ARG AQ_REVISION=//p' dockerfiles/aq.Dockerfile | cut -c1-7)
IMAGE="ghcr.io/kestra-io/aq:$REVISION"
docker pull "$IMAGE" || docker build -f dockerfiles/aq.Dockerfile -t "$IMAGE" .
