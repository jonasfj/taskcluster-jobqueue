
run:
	python3.3 src/jobqueue.py

test:
	PYTHONPATH='src' python3.3 -m unittest discover -s tests -p "*_test.py"
