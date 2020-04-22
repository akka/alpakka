#!/bin/bash

set -x

CURR_DIR=$(dirname "$(readlink -f "$0")")

echo "travis.sh: stage=$TRAVIS_BUILD_STAGE_NAME job=$TRAVIS_JOB_NAME"
if [ "$TRAVIS_BUILD_STAGE_NAME" = "test" ] && [ ! -z "$TRAVIS_JOB_NAME" ]; then
   echo "Running test if changed"
   $CURR_DIR/test-if-changed.sh "$TRAVIS_JOB_NAME" "${PRE_CMD:=echo NOOP}" "+$TRAVIS_JOB_NAME/testChanged"
elif [ ! -z "$CMD" ]; then
   echo "Running sbt command '$CMD'"
   $CURR_DIR/build.sh "${PRE_CMD:=echo NOOP}" "$CMD"
else
  echo "Building nothing."
fi
