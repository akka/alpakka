#!/bin/bash

set -x

DIR=$1

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

git diff "$COMPARE_TO" --exit-code --quiet "$DIR" build.sbt project/
#git diff "$COMPARE_TO" --exit-code --quiet "$DIR" build.sbt project/ .github/workflows/
DIFF_EXIT_CODE=$?

if [ "$GITHUB_EVENT_NAME" == "schedule" ]
then
  echo "Building everything because nightly"
  echo "execute_build=true" >> $GITHUB_ENV
elif [ "$DIFF_EXIT_CODE" -eq 1 ]
then
  echo "Changes in ${DIR}"
  echo "execute_build=true" >> $GITHUB_ENV
else
  echo "No changes in $DIR"
  echo "execute_build=false" >> $GITHUB_ENV
fi
