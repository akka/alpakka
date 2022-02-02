#!/bin/bash

set -x

DIR=$1
PRE_CMD=$2
CMD=$3

if [ "$GITHUB_EVENT_NAME" == "pull_request" ]
then
  COMPARE_TO="origin/master"
else
  # for non-pr (master) builds, comparison for changes is made against the previous commit,
  # which might be:
  # * merge commit, then HEAD^ is the last commit before branching out
  # * squash commit, then HEAD^ is the previous state of master
  COMPARE_TO="HEAD^"
fi

git diff "$COMPARE_TO" --exit-code --quiet "$DIR" build.sbt project/ .github/workflows/
DIFF_EXIT_CODE=$?

if [ "$GITHUB_EVENT_NAME" == "schedule" ]
then
  echo "Building everything because nightly"
elif [ "$DIFF_EXIT_CODE" -eq 1 ]
then
  echo "Changes in ${DIR}"
else
  echo "No changes in $DIR"
  exit 0
fi

CURR_DIR=$(dirname "$(readlink -f "$0")")

$PRE_CMD
sbt "$CMD"
