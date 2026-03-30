#!/bin/bash

case ${1} in
    "linux/amd64")   echo "x86_64-unknown-linux-musl" ;;
    "linux/arm64")   echo "aarch64-unknown-linux-musl" ;;
    "linux/arm")     echo "armv7-unknown-linux-musleabihf" ;;
    "linux/riscv64") echo "riscv64gc-unknown-linux-musl" ;;
    *)               echo "Unsupported architecture: ${1}" >&2; exit 1 ;;
esac