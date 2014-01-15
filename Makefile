
database:
	rm -f sql/jobqueue.db
	sqlite3 sql/jobqueue.db < sql/schema.sql

run:
	python3.3 src/jobqueue.py

test:
	PYTHONPATH='src' python3.3 -m unittest discover -s tests -p "*_test.py"
