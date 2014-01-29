taskcluster-jobqueue
====================

Running the Job Queue
---------------------

Run the Job Queue as follows:
```
python3.3 jobqueue.py
```
or
```
make run
```

Using -h will show the currently supported command line options.

Running the Tests
There is a makefile target to run the unittests:
```
make test
```
There is also a stress test which will simulate running a configurable number of jobs. It is also a good example of how to use the REST API.
```
python3.3 stresstest/stresstest.py
```

Using -h will show the currently supported command line options.


REST API
--------

The following table summarizes the current REST API. Each job has a UUID assigned to it when the job is created by calling /job/new. The UUID is formatted as hexadecimal text with hypens, e.g. 4b7acb00-8860-11e3-9f64-606720020792.

Endpoint|Method|Purpose|Description
--------|------|-------|-----------
/0.1.0/job/new|POST|Create a new job|Creates a new job based on a json object. Returns a json object containing the uuid for the new job. The new job is created in the PENDING state.
/0.1.0/job/*uuid*|GET|Get job status|Returns a json object for the job with the specified uuid
/0.1.0/job/*uuid*/cancel|POST|Cancel a job|Cancels the job with the specified uuid.
/0.1.0/job/*uuid*/claim|POST|Claim a job|Claims the job with the specified uuid. This changes the state of the job from PENDING to RUNNING.
/0.1.0/job/*uuid*/finish|POST|Finish a job|Finish the job with the specified uuid. This changes the state of the job from RUNNING to FINISHED.
/0.1.0/jobs|GET|Get jobs|Get list of PENDING and RUNNING jobs. Optionally, the desired state can be specified, e.g. /0.1.0/jobs?state=PENDING will return only pending jobs.

Job queue implementation
------------------------

When the queue is running tasks can be posted with
`curl -X POST -d @task.json http://localhost:8314/0.1.0/job/new`
Where `task.json` is a file containing a task definition.
