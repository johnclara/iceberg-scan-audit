#!/bin/bash --norc
#
# Creates a new release version and tags the branch with that version
#

set -e


PROJECT=$1
SNAPSHOT_VERSION=`grep version= gradle.properties | grep -m1 -e '-SNAPSHOT' | egrep -o '[0-9]+\.[0-9]+(\.[0-9]+)?'`

echo $SNAPSHOT_VERSION

if [[ -z "$SNAPSHOT_VERSION" ]]; then
  echo "Could not find a SNAPSHOT version in gradle.properties, can only create RELEASE version from snapshot" >&2
  exit 1
fi

git checkout master
git pull origin master
RELEASE_VERSION=$SNAPSHOT_VERSION-RELEASE
echo "checking out new branch $RELEASE_VERSION..."
git checkout -b "$RELEASE_VERSION"
sed -i  -e 's/SNAPSHOT/RELEASE/' gradle.properties
echo "Starting release of version '$RELEASE_VERSION'"

VERSION_TAG=$RELEASE_VERSION
COMMENT="Creating release candidate for $RELEASE_VERSION"
git commit -a -m "$COMMENT"
git push origin $RELEASE_VERSION

git checkout master
NEW_SNAPSHOT_VERSION=`echo $SNAPSHOT_VERSION | awk -F. '{$NF = $NF + 1;} 1' | sed 's/ /./g'`
sed -i -e "s/$SNAPSHOT_VERSION/$NEW_SNAPSHOT_VERSION/" gradle.properties
COMMENT="bumping up snapshot version to $NEW_SNAPSHOT_VERSION"
git commit -a -m "$COMMENT"
git push origin master


