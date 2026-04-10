#!/bin/bash
trap 'kill 0' SIGINT SIGTERM

rm -r ./tmp/*

mkdir ./tmp ./tmp/1 ./tmp/2 ./tmp/3

./cache_cat --conf node1.toml &
sleep 1
./cache_cat --conf node2.toml &
sleep 1
./cache_cat --conf node3.toml &

wait