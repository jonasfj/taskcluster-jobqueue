import unittest
import json
import re
import sqlite3
import sys 
sys.path.append('../src')
from jobqueue import Job

class TestJob(unittest.TestCase):

    def test_init(self):
        job = Job()

        # we got a valid job id 
        self.assertIsNotNone(re.match('\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', str(job.job_id)))

        # jobs are created pending
        job.pending()
        self.assertEqual(job.state, Job.PENDING)
        self.assertIsNotNone(job.entered_queue_time)

        # if priority not specified, initialized to default
        self.assertEqual(job.priority, Job.DEFAULT_PRIORITY)

        # specify priority
        job2 = Job(job_object={'priority': 42})
        self.assertEqual(job2.priority, 42)

    def test_sorting(self):
        # jobs with equal priority should be sorted by age
        job = Job()
        job.pending()
        job2 = Job()
        job2.pending()
        self.assertTrue(job < job2)

        # jobs should be sorted by priority
        job = Job(job_object={'priority': 2})
        job.pending()
        job2 = Job(job_object={'priority': 1})
        job2.pending()
        self.assertTrue(job2 < job)

    def test_json(self):
        # jobs should be json serializable
        job = Job()
        json.dumps(json.dumps(job.job_object))

    # TODO: database tests

if __name__ == '__main__':
    unittest.main()
