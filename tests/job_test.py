import unittest
import re
import sys 
sys.path.append('../src')
from jobqueue import Job

class TestJob(unittest.TestCase):

    def test_init(self):
        job = Job()

        # we got a valid uuid
        self.assertIsNotNone(re.match('\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', job.uuid))

        # jobs are created pending
        self.assertEqual(job.state, Job.PENDING)

        # if priority not specified, initialized to default
        self.assertEqual(job.priority, Job.DEFAULT_PRIORITY)

        # jobs are created with valid queue time
        self.assertIsNotNone(job.entered_queue_time)

        # specify priority
        job2 = Job(42)
        self.assertEqual(job2.priority, 42)

    def test_sorting(self):
        # jobs with equal priority should be sorted by age
        job = Job()
        job2 = Job()
        self.assertTrue(job < job2)

        # jobs should be sorted by priority
        job = Job(2)
        job2 = Job(1)
        self.assertTrue(job2 < job)

if __name__ == '__main__':
    unittest.main()
