#!/bin/bash

set -x

CURR_DIR=$(dirname "$(readlink -f "$0")")

if [ "$TRAVIS_BUILD_STAGE_NAME" = "Test" ] && [ ! -z "$TRAVIS_JOB_NAME" ]; then
   echo "Running test if changed"
   $CURR_DIR/test-if-changed.sh "$TRAVIS_JOB_NAME" "${PRE_CMD:=echo NOOP}" "+$TRAVIS_JOB_NAME/testChanged"
elif [ ! -z "$CMD" ]; then
   echo "Running sbt command '$CMD'"
   $CURR_DIR/build.sh "${PRE_CMD:=echo NOOP}" "$CMD"
else
  echo "Building nothing."
fi
