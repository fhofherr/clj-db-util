#!/bin/bash

if [[ $EUID -ne 0 ]]
then
    echo "$0 must be run by root."
    exit 1
fi

BASEDIR=$PWD

# Install test system including required databases.
# For java installation see http://www.webupd8.org/2012/09/install-oracle-java-8-in-ubuntu-via-ppa.html
echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" > /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

add-apt-repository ppa:webupd8team/java

apt-get update

echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections

apt-get install -y postgresql-9.5 postgresql-client-9.5 oracle-java8-installer oracle-java8-set-default unzip git
locale-gen

# Install leiningen
wget -O /usr/local/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod a+rx /usr/local/bin/lein

# Create a postgresql database for testing
if [ ! -f "/vagrant/scripts/vagrant/create_postgresdb.sh" ]
then
    echo "Can't create postgres database."
    exit 1
fi
/vagrant/scripts/vagrant/create_postgresdb.sh

# Install simple
git clone https://github.com/fhofherr/simple "$BASEDIR/simple"
cd "$BASEDIR/simple"
/usr/local/bin/lein uberjar
cp target/uberjar/simple*.jar /usr/local/lib/simple.jar
chmod a+r /usr/local/lib/simple.jar
cd $BASEDIR
rm -rf "$BASEDIR/simple"
