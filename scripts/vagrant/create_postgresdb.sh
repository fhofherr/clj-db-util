#!/bin/bash

if [ ! -f "/vagrant/scripts/vagrant/pg_env.sh" ]
then
    echo "Can't source environment configuration"
    exit 1
fi
source /vagrant/scripts/vagrant/pg_env.sh

sudo -u postgres psql -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASS';"
sudo -u postgres psql -c "CREATE DATABASE $DB_NAME WITH OWNER $DB_USER ENCODING 'UTF-8';"
