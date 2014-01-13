
run:
	python src/jobqueue.py

test:
	PYTHONPATH='src' python -m unittest discover -s tests -p "*_test.py"
