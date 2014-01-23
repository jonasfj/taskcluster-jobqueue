#!/bin/bash

# start postgres
/bin/su postgres -c '/usr/lib/postgresql/9.3/bin/postgres \
    -D /var/lib/postgresql/9.3/main \
    -c config_file=/etc/postgresql/9.3/main/postgresql.conf &'
sleep 2

cd jobqueue

# initialize database
cd sql
/bin/su postgres -c "./createdb.sh"
cd ..

# start rabbitmq
/usr/sbin/rabbitmq-server &
sleep 2

# run jobqueue 
python3.2 src/jobqueue.py &

# run tests
PYTHONPATH='src' python3.2 -m unittest discover -s tests -p "*_test.py"
