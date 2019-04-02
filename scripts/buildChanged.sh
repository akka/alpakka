#!/bin/bash

DIR=$1
JDK=$2
PRE_CMD=$3
CMD=$4

git diff origin/master --exit-code --quiet $DIR

if [ $? -eq 1 ]
then
  echo "Changes in ${DIR}"
  ./jabba use ${JDK}
  java -version
  $PRE_CMD
  sbt -jvm-opts .jvmopts-travis "$CMD"
else
  echo "No changes in ${DIR}"
  exit 0
fi
