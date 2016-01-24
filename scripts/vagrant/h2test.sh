#!/bin/bash

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
git pull

source /vagrant/scripts/vagrant/h2_env.sh
lein with-profile +h2 test :integration
