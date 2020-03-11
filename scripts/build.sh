#!/bin/bash

set -x

PRE_CMD=$1
CMD=$2

[ "$TRAVIS_EVENT_TYPE" == "cron" ] && JDK="adopt@~1.11.0-4" || JDK="adopt@~1.8.0-222"

# using jabba for custom jdk management
curl -sL https://raw.githubusercontent.com/shyiko/jabba/0.11.2/install.sh | bash
. ~/.jabba/jabba.sh
jabba install "$JDK"
jabba use "$JDK"
java -version
$PRE_CMD

sbt -jvm-opts .jvmopts-travis "$CMD"