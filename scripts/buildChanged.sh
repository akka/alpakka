#!/bin/bash

DIR=$1
JDK=$2
PRE_CMD=$3
CMD=$4

git diff origin/master --exit-code --quiet $DIR build.sbt project/ .travis.yml

if [ $? -eq 1 ]
then
  echo "Changes in ${DIR}"
  # using jabba for custom jdk management
  curl -sL https://raw.githubusercontent.com/shyiko/jabba/0.11.2/install.sh | bash
  . ~/.jabba/jabba.sh
  ./jabba install ${JDK}
  ./jabba use ${JDK}
  java -version
  $PRE_CMD
  sbt -jvm-opts .jvmopts-travis "$CMD"
else
  echo "No changes in ${DIR}"
  exit 0
fi
