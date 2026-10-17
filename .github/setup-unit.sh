#!/bin/sh
set -e

# the Arrow tests run aq in this image, build it when it is not published yet
docker pull ghcr.io/kestra-io/aq:latest || docker build -f dockerfiles/aq.Dockerfile -t ghcr.io/kestra-io/aq:latest .
