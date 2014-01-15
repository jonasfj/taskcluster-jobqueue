import unittest
import os
import random
import sys
sys.path.append('../src')
from jobqueue import Job, JobQueue
import util

class TestJobQueue(unittest.TestCase):

    def setUp(self):
        self.job = {'version': '0.1.0', 'priority': 0}
        self.db = util.make_temporary_database() 

    def tearDown(self):
        self.db.close()

    def test_equal_priority(self):
        num_jobs = 10

        jobq = JobQueue(self.db.name)

        jobs = []
        for i in range(0, num_jobs):
            job = Job(self.db.name, self.job)
            job.pending()
            jobq.add_job_to_pending_queue(job)
            jobs.append(job)

        # should be FIFO
        for i in range(0, num_jobs):
            job = jobq.pop_job_from_pending_queue()
            self.assertEqual(jobs[i].job_id, job.job_id)

    def test_random_priority(self):
        num_jobs = 100

        jobq = JobQueue(self.db.name)

        jobs = []
        priorities = [x for x in range(0, num_jobs)]
        random.shuffle(priorities)
        for i in range(0, num_jobs):
            job = Job(self.db.name, {'priority': priorities[i]})
            job.pending()
            jobq.add_job_to_pending_queue(job)
            jobs.append(job)

        jobs = sorted(jobs)
        for i in range(0, num_jobs):
            job = jobq.pop_job_from_pending_queue()
            self.assertEqual(jobs[i].job_id, job.job_id)

    def test_pendingqueue_underflow(self):
        jobq = JobQueue(self.db.name)
        self.assertIsNone(jobq.pop_job_from_pending_queue())
    
    # TODO: heartbeating tests 

if __name__ == '__main__':
    unittest.main()
