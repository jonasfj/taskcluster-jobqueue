import unittest
import re
import sys 
sys.path.append('../src')
from jobqueue import Job, JobQueue

class TestJobQueue(unittest.TestCase):

    def test_pending_queue(self):

        jobq = JobQueue()

        # jobs with equal priority should be FIFO
        num_jobs = 100
        jobs = []
        for i in xrange(0, num_jobs):
            job = Job()
            jobs.append(job)
            jobq.add_job_to_pending_queue(job)

        for i in xrange(0, num_jobs):
            job = jobq.remove_job_from_pending_queue()
            self.assertIs(jobs[i], job)

    def test_heartbeating(self):
        pass

if __name__ == '__main__':
    unittest.main()
