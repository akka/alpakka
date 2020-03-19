#!/bin/bash

VERSION=$1
BINARY_VERSION=$2
if [ -z $BINARY_VERSION ]
then
  echo specify the version name to be released and its minor version, eg. 2.0.0 2.0
else
  sed -e 's/\$VERSION\$/'$VERSION'/g' docs/release-train-issue-template.md > /tmp/release-$VERSION.tmp
  sed -e 's/\$BINARY_VERSION\$/'$BINARY_VERSION'/g' /tmp/release-$VERSION.tmp > /tmp/release-$VERSION.md
  echo Created $(hub issue create -F /tmp/release-$VERSION.md -M $VERSION --browse)
fi
