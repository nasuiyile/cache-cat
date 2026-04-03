#!/bin/bash

for TARGET_NAME in $@; do

    case ${TARGET_NAME} in
        "linux/amd64")   
            export TARGET_PLATFORM="x86_64-unknown-linux-musl" 
            ;;
        "linux/arm64")   
            export TARGET_PLATFORM="aarch64-unknown-linux-musl" 
            ;;
        "linux/arm" | "linux/arm/v7")     
            export TARGET_PLATFORM="armv7-unknown-linux-musleabihf" 
            ;;
        "linux/riscv64") 
            export TARGET_PLATFORM="riscv64gc-unknown-linux-musl" 
            ;;
        *)               
            echo "Unsupported architecture: ${TARGET_NAME}" >&2
            exit 1 ;
    esac

    rustup target add ${TARGET_PLATFORM}

    cargo zigbuild --release --target ${TARGET_PLATFORM}

    mkdir -p target/dist/${TARGET_NAME}

    cp target/${TARGET_PLATFORM}/release/core_raft target/dist/${TARGET_NAME}/
done