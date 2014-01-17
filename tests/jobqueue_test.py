import unittest
import os
import psycopg2
import random
import sys
sys.path.append('../src')
from jobqueue import Job, JobQueue
import util

class TestJobQueue(unittest.TestCase):

    def setUp(self):
        self.job = {'version': '0.1.0', 'priority': 0}
        self.dbpath = 'dbname=jobqueue user=jobqueue host=localhost password=jobqueue'
        self.dbconn = psycopg2.connect(self.dbpath)
        cursor = self.dbconn.cursor()
        cursor.execute('delete from Job');
        cursor.execute('delete from JobQueueJob');
        cursor.execute('delete from Worker');
        self.dbconn.commit()

    def tearDown(self):
        self.dbconn.close()
        pass

    def test_equal_priority(self):
        num_jobs = 100

        jobq = JobQueue(self.dbpath)

        jobs = []
        for i in range(0, num_jobs):
            job = Job(self.dbconn, self.job)
            job.pending(None)
            jobq.add_job_to_pending_queue(self.dbconn, job)
            jobs.append(job)

        # should be FIFO
        for i in range(0, num_jobs):
            job = jobq.pop_job_from_pending_queue(self.dbconn)
            self.assertEqual(jobs[i].job_id, job.job_id)

    def test_random_priority(self):
        num_jobs = 100

        jobq = JobQueue(self.dbpath)

        jobs = []
        priorities = [x for x in range(0, num_jobs)]
        random.shuffle(priorities)
        for i in range(0, num_jobs):
            job = Job(self.dbconn, {'priority': priorities[i]})
            job.pending(self.dbconn)
            jobq.add_job_to_pending_queue(self.dbconn, job)
            jobs.append(job)

        jobs = sorted(jobs)
        for i in range(0, num_jobs):
            job = jobq.pop_job_from_pending_queue(self.dbconn)
            self.assertEqual(jobs[i].job_id, job.job_id)

    def test_pendingqueue_underflow(self):
        jobq = JobQueue(self.dbpath)
        self.assertIsNone(jobq.pop_job_from_pending_queue(self.dbconn))

    # TODO: heartbeating tests 

if __name__ == '__main__':
    unittest.main()
