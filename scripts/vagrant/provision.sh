#!/bin/bash

if [[ $EUID -ne 0 ]]
then
    echo "$0 must be run by root."
    exit 1
fi

if [ ! -f "/vagrant/scripts/vagrant/env.sh" ]
then
    echo "Can't source environment configuration"
    exit 1
fi
source /vagrant/scripts/vagrant/env.sh

# Install test system including required databases.
# For java installation see http://www.webupd8.org/2012/09/install-oracle-java-8-in-ubuntu-via-ppa.html
echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" > /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

add-apt-repository ppa:webupd8team/java

apt-get update

echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections

apt-get install -y postgresql-9.5 postgresql-client-9.5 oracle-java8-installer oracle-java8-set-default unzip
locale-gen

# Install leiningen
wget -O /usr/local/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod a+rx /usr/local/bin/lein

# Create a postgresql database for testing
sudo -u postgres psql -c "CREATE USER $PG_DB_USER WITH PASSWORD '$PG_DB_PASS';"
sudo -u postgres psql -c "CREATE DATABASE $PG_DB_NAME WITH OWNER $PG_DB_USER ENCODING 'UTF-8';"
