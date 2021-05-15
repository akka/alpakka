#!/bin/bash

set -x

PRE_CMD=$1
CMD=$2

[ "$TRAVIS_EVENT_TYPE" == "cron" ] && JDK="adopt@~1.11-0" || JDK="adopt@~1.8-0"

# using jabba for custom jdk management
if [ ! -f ~/.jabba/jabba.sh ]; then curl -L -v --retry 5 -o jabba-install.sh https://raw.githubusercontent.com/shyiko/jabba/0.11.2/install.sh && bash jabba-install.sh; fi
. ~/.jabba/jabba.sh
jabba install "$JDK"
jabba use "$JDK"
java -version
$PRE_CMD

sbt -sbt-launch-repo https://repo1.maven.org/maven2 -jvm-opts .jvmopts-travis "$CMD"
