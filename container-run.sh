#!/bin/bash

# start postgres
/bin/su postgres -c '/usr/lib/postgresql/9.3/bin/postgres \
    -D /var/lib/postgresql/9.3/main \
    -c config_file=/etc/postgresql/9.3/main/postgresql.conf &'
sleep 2

# initialize database
cd sql; ./createdb.sh

# start rabbitmq
/usr/sbin/rabbitmq-server &
sleep 2

# run jobqueue 
python3.2 ../src/jobqueue.py
