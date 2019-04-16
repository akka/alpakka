#!/bin/bash

DIR=$1
JDK=$2
PRE_CMD=$3
CMD=$4

if [ "$TRAVIS_PULL_REQUEST" ]
then
  COMPARE_TO="origin/master"
else
  # for non-pr (master) builds, comparison for changes is made against the previous commit,
  # which might be:
  # * merge commit, then HEAD^ is the last commit before branching out
  # * squash commit, then HEAD^ is the previous state of master
  COMPARE_TO="HEAD^"
fi

git diff "$COMPARE_TO" --exit-code --quiet "$DIR" build.sbt project/ .travis.yml

if [ $? -eq 1 ]
then
  echo "Changes in ${DIR}"
  # using jabba for custom jdk management
  curl -sL https://raw.githubusercontent.com/shyiko/jabba/0.11.2/install.sh | bash
  . ~/.jabba/jabba.sh
  jabba install "$JDK"
  jabba use "$JDK"
  java -version
  $PRE_CMD
  sbt -jvm-opts .jvmopts-travis "$CMD"
else
  echo "No changes in $DIR"
  exit 0
fi
