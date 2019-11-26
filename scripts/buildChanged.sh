#!/bin/bash

DIR=$1
JDK=$2
PRE_CMD=$3
CMD=$4

if [ "$TRAVIS_PULL_REQUEST" == "false" ]
then
  # for non-pr (master) builds, comparison for changes is made against the previous commit,
  # which might be:
  # * merge commit, then HEAD^ is the last commit before branching out
  # * squash commit, then HEAD^ is the previous state of master
  COMPARE_TO="HEAD^"
else
  COMPARE_TO="origin/master"
fi

git diff "$COMPARE_TO" --exit-code --quiet "$DIR" build.sbt project/ .travis.yml
DIFF_EXIT_CODE=$?

if [ "$TRAVIS_EVENT_TYPE" == "cron" ]
then
  echo "Building everything because nightly"
elif [ "$DIFF_EXIT_CODE" -eq 1 ]
then
  echo "Changes in ${DIR}"
else
  echo "No changes in $DIR"
  exit 0
fi

# using jabba for custom jdk management
curl -sL https://raw.githubusercontent.com/shyiko/jabba/0.11.2/install.sh | bash
. ~/.jabba/jabba.sh
jabba install "$JDK"
jabba use "$JDK"
java -version
$PRE_CMD
sbt -Dsbt.color=always -sbt-jvm-opts .jvmopts-travis "$CMD"
