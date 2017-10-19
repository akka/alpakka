#!/bin/bash

GEODE_DATA=/geode-data

rm -rf $GEODE_DATA
mkdir -p $GEODE_DATA/locator
mkdir -p $GEODE_DATA/server


gfsh -e "start locator --name=$HOSTNAME-locator --dir=$GEODE_DATA/locator --mcast-port=0 --hostname-for-clients=0.0.0.0" -e "start server --name=$HOSTNAME-server --locators=localhost[10334] --dir=$GEODE_DATA/server --server-port=40404 --max-heap=1G --hostname-for-clients=0.0.0.0 --cache-xml-file=/scripts/cache.xml"

while true; do
    sleep 10
done

