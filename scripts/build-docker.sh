#!/bin/bash

# DOCKER_TARGETS="linux/amd64 linux/arm64 linux/riscv64"
DOCKER_TARGETS="linux/amd64"

./scripts/build-multitarget.sh ${DOCKER_TARGETS}

docker buildx build --load \
    --platform $(echo $DOCKER_TARGETS| tr ' ' ',') \
    -t cache-cat:latest \
    .