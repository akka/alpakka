#!/bin/sh
# ---------- helper script to separate log files in travis build
printf "\n\n\n%s\n\n" $1
cat $1
