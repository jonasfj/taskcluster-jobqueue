import datetime
import psycopg2
import json
import re
import sys
sys.path.append('../src')
from jobqueue import Job
import unittest
import util

class TestJob(unittest.TestCase):

    def setUp(self):
        self.dbconn = psycopg2.connect('dbname=jobqueue user=jobqueue host=localhost password=jobqueue')
        cursor = self.dbconn.cursor()
        cursor.execute('delete from Job');
        cursor.execute('delete from JobQueueJob');
        cursor.execute('delete from Worker');

    def tearDown(self):
        self.dbconn.close()

    def test_init(self):
        job = Job()

        # we got a valid job id
        self.assertIsNotNone(re.match('\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', str(job.job_id)))

        # jobs are created pending
        job.pending(None)
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
        job.pending(None)
        job2 = Job()
        job2.pending(None)
        self.assertTrue(job < job2)

        # jobs should be sorted by priority
        job = Job(job_object={'priority': 2})
        job.pending(None)
        job2 = Job(job_object={'priority': 1})
        job2.pending(None)
        self.assertTrue(job2 < job)

    def test_json(self):
        # jobs should be json serializable
        job = Job()
        json_job = json.loads(job.get_json())
        self.assertEqual(json_job['job_id'], str(job.job_id))
        self.assertEqual(json_job['job_object'], job.job_object)
        self.assertEqual(json_job['state'], job.state)
        self.assertEqual(json_job['priority'], job.priority)
        self.assertEqual(json_job['max_pending_seconds'], job.max_pending_seconds)
        self.assertEqual(json_job['max_runtime_seconds'], job.max_runtime_seconds)
        self.assertEqual(json_job['entered_queue_time'], job.entered_queue_time)
        self.assertEqual(json_job['started_running_time'], job.started_running_time)
        self.assertEqual(json_job['finished_time'], job.finished_time)
        self.assertEqual(json_job['last_heartbeat_time'], job.last_heartbeat_time)
        self.assertEqual(json_job['missed_heartbeats'], job.missed_heartbeats)
        self.assertEqual(json_job['worker_id'], job.worker_id)
        self.assertEqual(json_job['job_results'], job.job_results)

    def test_heartbeat(self):
        job = Job()
        self.assertIs(job.last_heartbeat_time, None)

        # heartbeat should give us a datetime
        job.heartbeat(None)
        self.assertEqual(type(job.last_heartbeat_time), type(datetime.datetime.now()))

        # heartbeats should increase
        last_heartbeat = job.last_heartbeat_time
        job.heartbeat(None)
        self.assertTrue(job.last_heartbeat_time > last_heartbeat)

        # heartbeat should match in json
        json_job = json.loads(job.get_json())
        self.assertEqual(json_job['last_heartbeat_time'], str(job.last_heartbeat_time))

        # heartbeat should match after database round trip
        job = Job(self.dbconn)
        db_job = Job.locate(job.job_id, self.dbconn)
        self.assertEqual(job.last_heartbeat_time, db_job.last_heartbeat_time)
        job.heartbeat(self.dbconn)
        db_job = Job.locate(job.job_id, self.dbconn)
        self.assertEqual(job.last_heartbeat_time, db_job.last_heartbeat_time)

    def test_database_roundtrip(self):
        # job should not change on round trip from database
        job = Job(self.dbconn)
        db_job = Job.locate(job.job_id, self.dbconn)
        for field in job.__dict__:
            self.assertEqual(job.__dict__[field], db_job.__dict__[field], 'field %s does not match' % field)

        job.pending(self.dbconn)
        db_job = Job.locate(job.job_id, self.dbconn)
        for field in job.__dict__:
            self.assertEqual(job.__dict__[field], db_job.__dict__[field], 'field %s does not match' % field)

        job.run(self.dbconn, 0)
        db_job = Job.locate(job.job_id, self.dbconn)
        for field in job.__dict__:
            self.assertEqual(job.__dict__[field], db_job.__dict__[field], 'field %s does not match' % field)

        job.finish(self.dbconn)
        db_job = Job.locate(job.job_id, self.dbconn)
        for field in job.__dict__:
            self.assertEqual(job.__dict__[field], db_job.__dict__[field], 'field %s does not match' % field)

if __name__ == '__main__':
    unittest.main()
