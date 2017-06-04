#!/bin/bash

rm -rf /geode
mkdir -p /geode/locator
mkdir -p /geode/server


gfsh -e "start locator --name=$HOSTNAME-locator --dir=/geode/locator --mcast-port=0 --hostname-for-clients=0.0.0.0" -e "start server --name=$HOSTNAME-server --locators=localhost[10334] --dir=/geode/server --server-port=40404 --max-heap=1G --hostname-for-clients=0.0.0.0 --cache-xml-file=/scripts/cache.xml"

while true; do
    sleep 10
done

