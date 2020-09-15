#!/bin/bash

set -x

PRE_CMD=$1
CMD=$2

[ "$TRAVIS_EVENT_TYPE" == "cron" ] && JDK="adopt@~1.11-0" || JDK="adopt@~1.8-0"

~/.jabba/bin/jabba install "$JDK"
~/.jabba/bin/jabba use "$JDK"
java -version
$PRE_CMD

sbt -jvm-opts .jvmopts-travis "$CMD"
