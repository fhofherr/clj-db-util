#!/bin/bash

cd /vagrant
CUR_HASH=$(git rev-parse HEAD)

if [ -z "$WORKSPACE" ]
then
    WORKSPACE="/tmp/clj-db-util/h2test"
fi
mkdir -p $WORKSPACE

if [ ! -e "$WORKSPACE/clj-db-util" ]
then
    git clone /vagrant "$WORKSPACE/clj-db-util"
fi

cd $WORKSPACE/clj-db-util
git fetch -p
git reset --hard $CUR_HASH

source /vagrant/scripts/vagrant/h2_env.sh
lein with-profile +h2 test :integration
