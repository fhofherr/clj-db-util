#!/bin/bash

if [ -z "$WORKSPACE" ]
then
    WORKSPACE="/tmp/clj-db-util/unittest"
fi
mkdir -p $WORKSPACE


if [ ! -e "$WORKSPACE/clj-db-util" ]
then
    git clone /vagrant "$WORKSPACE/clj-db-util"
fi

cd $WORKSPACE/clj-db-util
git pull

lein test
