#!/bin/bash

set -x

PRE_CMD=$1
CMD=$2

$PRE_CMD

sbt -sbt-launch-repo https://repo1.maven.org/maven2 -jvm-opts .jvmopts-travis "$CMD"
