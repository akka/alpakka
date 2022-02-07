#!/bin/bash

# set -x

CONNECTOR=$1

if [ "$GITHUB_EVENT_NAME" == "pull_request" ]
then
  COMPARE_TO="origin/${GITHUB_BASE_REF}"
else
  # for non-pr (master) builds, comparison for changes is made against the previous commit,
  # which might be:
  # * merge commit, then HEAD^ is the last commit before branching out
  # * squash commit, then HEAD^ is the previous state of master
  COMPARE_TO="HEAD^"
fi

git diff "$COMPARE_TO" --exit-code --quiet "${CONNECTOR}" build.sbt project/ .github/workflows/
DIFF_EXIT_CODE=$?

# Setting the `execute_build` env variable for other job steps to use
# https://docs.github.com/en/actions/using-workflows/workflow-commands-for-github-actions#setting-an-environment-variable
if [ "$GITHUB_EVENT_NAME" == "schedule" ]
then
  echo "Building everything as it is a scheduled build"
  echo "execute_build=true" >> $GITHUB_ENV
elif [ "$DIFF_EXIT_CODE" -eq 1 ]
then
  echo "Changes in Alpakka ${CONNECTOR}"
  echo "execute_build=true" >> $GITHUB_ENV
else
  echo "No changes in Alpakka ${CONNECTOR}"
  echo "execute_build=false" >> $GITHUB_ENV
fi
