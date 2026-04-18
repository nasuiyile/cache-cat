#!/bin/bash
trap 'kill 0' SIGINT SIGTERM

rm -r ./tmp/*

mkdir ./tmp ./tmp/1

./cache_cat --conf node1.toml &


wait