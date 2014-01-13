import unittest
import random
import sys 
sys.path.append('../src')
from jobqueue import Job, JobQueue

class TestJobQueue(unittest.TestCase):

    def test_pending_queue(self):
        num_jobs = 10000

        # jobs with equal priority should be FIFO
        jobq = JobQueue()

        jobs = []
        for i in xrange(0, num_jobs):
            job = Job()
            jobs.append(job)
            jobq.add_job_to_pending_queue(job)

        for i in xrange(0, num_jobs):
            job = jobq.remove_job_from_pending_queue()
            self.assertIs(jobs[i], job)

        # otherwise, jobs should be sorted based upon priority 
        jobq = JobQueue()

        jobs = []
        priorities = range(0, num_jobs)
        random.shuffle(priorities)
        for i in xrange(0, num_jobs):
            job = Job(priorities[i])
            jobs.append(job)
            jobq.add_job_to_pending_queue(job)

        jobs = sorted(jobs)
        for i in xrange(0, num_jobs):
            job = jobq.remove_job_from_pending_queue()
            self.assertIs(jobs[i], job)

        # underflow 
        jobq = JobQueue()
        self.assertIsNone(jobq.remove_job_from_pending_queue())
    
    # TODO: database tests, once there is a database
    #       heartbeating tests

if __name__ == '__main__':
    unittest.main()
