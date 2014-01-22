from amqplib import client_0_8 as amqp
import sys
sys.path.append('../src')

import http
import json
import psycopg2
import unittest
import subprocess
import threading
import time
from wsgiref.simple_server import make_server
import socket

import jobqueue
import util

def get_json(response):
    if response.status != 200:
        print('error: bad http status: %d' % response.status)
        return {}

    text = response.read().decode().strip()

    try:
        decoded = json.loads(text)
    except ValueError:
        print('could not decode: ' + text)
        return {}

    return decoded

def wait_for_job(rabbit_chan):
    msg = rabbit_chan.basic_get(queue='jobs', no_ack=True)
    while not msg:
        os.sleep(1)
        print('.')
        msg = rabbit_chan.basic_get(queue='jobs', no_ack=True)

    if msg:
        return msg.body
    else:
        return None

#TODO: test worker_id stuff

class TestJobQueueREST(unittest.TestCase):

    # JobQueue server instance running in its own thread
    httpd = None

    @classmethod
    def setUpClass(cls):
        dbpath = 'dbname=jobqueue user=jobqueue host=localhost password=jobqueue'
        dbconn = psycopg2.connect(dbpath)
        cursor = dbconn.cursor()
        cursor.execute('delete from Job');
        cursor.execute('delete from Worker');
        dbconn.commit()

        app = jobqueue.Application(dbpath)

        cls.port = util.find_open_port('127.0.0.1', 15807)
        cls.httpd = make_server('0.0.0.0', cls.port, app)
        thread = threading.Thread(target=cls.httpd.serve_forever)
        thread.daemon = True
        thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def setUp(self):
        self.conn = http.client.HTTPConnection('localhost', TestJobQueueREST.port)
        self.job = {'version': '0.1.0'}

        # clear database between tests
        self.dbconn = psycopg2.connect('dbname=jobqueue user=jobqueue host=localhost password=jobqueue')
        cursor = self.dbconn.cursor()
        cursor.execute('delete from Job');
        cursor.execute('delete from Worker');
        self.dbconn.commit()

        # set up rabbit connection
        self.rabbit_conn = amqp.Connection(host="localhost:5672", userid="guest", password="guest", virtual_host="/", insist=False)
        self.rabbit_chan = self.rabbit_conn.channel()

        # purge queue
        self.rabbit_chan.queue_purge(queue='jobs')

    def tearDown(self):
        self.conn.close()

    def test_new_job(self):
        jobs = []
        NUM_JOBS = 10

        # new jobs
        for i in range(0, NUM_JOBS):
            headers = {"Content-Type": "application/json",
                       "Content-Length": len(json.dumps(self.job))}
            self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
            resp = self.conn.getresponse()
            self.assertEqual(resp.status, 200)
            res = get_json(resp)
            self.assertIn('job_id', res)
            job = res['job_id']
            jobs.append(job)

        # new jobs should appear in jobs list
        self.conn.request('GET', '/0.1.0/jobs')
        res = get_json(self.conn.getresponse())
        res_uuids = [job['job_id'] for job in res]
        for job in jobs:
            self.assertTrue(job in res_uuids)

        # new job should be pending
        a_job = jobs[0]
        self.conn.request('GET', '/0.1.0/job/' + a_job + '/')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['state'], 'PENDING')

    def test_cancel_pending_job(self):
        # new job
        headers = {"Content-Type": "application/json",
                   "Content-Length": len(json.dumps(self.job))}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        a_job = res['job_id']

        # cancel job
        self.conn.request('POST', '/0.1.0/job/' + a_job + '/cancel')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # should be finished
        self.conn.request('GET', '/0.1.0/job/' + a_job + '/')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['state'], 'FINISHED')

        # should not appear in all jobs
        self.conn.request('GET', '/0.1.0/jobs')
        res = get_json(self.conn.getresponse())
        self.assertTrue(a_job not in res)

        # can't cancel unknown job uuid
        self.conn.request('POST', '/0.1.0/job/00000000-0000-0000-0000-000000000000/cancel')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 404)

    def test_cancel_running_job(self):
        # new job
        headers = {"Content-Type": "application/json",
                   "Content-Length": len(json.dumps(self.job))}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        a_job = res['job_id']

        # claim
        res = wait_for_job(self.rabbit_chan)
        self.assertIsNot(res, None)
        our_job = json.loads(res)['job_id']
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # cancel job
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/cancel')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # should be finished
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['state'], 'FINISHED')

        # should not appear in all jobs
        self.conn.request('GET', '/0.1.0/jobs')
        res = get_json(self.conn.getresponse())
        self.assertTrue(our_job not in res)

        # can't cancel unknown job uuid
        self.conn.request('POST', '/0.1.0/job/00000000-0000-0000-0000-000000000000/cancel')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 404)

    def test_job_claim(self):
        # new job
        headers = {"Content-Type": "application/json",
                   "Content-Length": len(json.dumps(self.job))}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        a_job = res['job_id']

        # claim
        res = wait_for_job(self.rabbit_chan)
        self.assertIsNot(res, None)
        our_job = json.loads(res)['job_id']
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # claimed job should be running
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['state'], 'RUNNING')

        # should not be in pending list
        self.conn.request('GET', '/0.1.0/jobs?state=PENDING')
        res = get_json(self.conn.getresponse())
        self.assertTrue(our_job not in [job['job_id'] for job in res])

        # should be in all jobs running list
        self.conn.request('GET', '/0.1.0/jobs?state=RUNNING')
        res = get_json(self.conn.getresponse())
        print(res)
        self.assertTrue(our_job in [job['job_id'] for job in res])

    def test_job_heartbeat(self):
        # new job
        headers = {"Content-Type": "application/json",
                   "Content-Length": len(json.dumps(self.job))}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        a_job = res['job_id']

        # claim
        res = wait_for_job(self.rabbit_chan)
        self.assertIsNot(res, None)
        our_job = json.loads(res)['job_id']
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # heartbeat initially None
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['last_heartbeat_time'], None)

        # heartbeat
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/heartbeat')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # heartbeat has changed
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/')
        res = get_json(self.conn.getresponse())
        self.assertNotEqual(res['last_heartbeat_time'], None)

        # can't complete bad job uuid
        self.conn.request('POST', '/0.1.0/job/00000000-0000-0000-0000-000000000000/heartbeat')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 404)

    def test_job_complete(self):
        # new job
        headers = {"Content-Type": "application/json",
                   "Content-Length": len(json.dumps(self.job))}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        a_job = res['job_id']

        # claim
        res = wait_for_job(self.rabbit_chan)
        self.assertIsNot(res, None)
        our_job = json.loads(res)['job_id']
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # complete
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/complete')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # should be finished
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['state'], 'FINISHED')

        # should no longer be in all jobs running list
        self.conn.request('GET', '/0.1.0/jobs?state=RUNNING')
        res = get_json(self.conn.getresponse())
        self.assertTrue(our_job not in [job['job_id'] for job in res])

        # can't complete bad job uuid
        self.conn.request('POST', '/0.1.0/job/00000000-0000-0000-0000-000000000000/complete')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 404)

        # can't complete finished job
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/complete')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 403)

        # can't complete pending job
        headers = {"Content-Type": "application/json",
                   "Content-Length": len(json.dumps(self.job))}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        a_job = res['job_id']
        self.conn.request('POST', '/0.1.0/job/' + a_job + '/complete')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 403)

    def test_claim_no_jobs(self):
        self.conn.request('POST', '/0.1.0/job/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 404)

    def test_badmethods(self):
        self.conn.request('GET', '/0.1.0/job/new')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)
        self.conn.request('POST', '/0.1.0/job/00000000-0000-0000-0000-000000000000/')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        self.conn.request('GET', '/0.1.0/job/00000000-0000-0000-0000-000000000000/cancel')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        self.conn.request('GET', '/0.1.0/job/00000000-0000-0000-0000-000000000000/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        self.conn.request('GET', '/0.1.0/job/00000000-0000-0000-0000-000000000000/heartbeat')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        self.conn.request('GET', '/0.1.0/job/00000000-0000-0000-0000-000000000000/complete')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        self.conn.request('POST', '/0.1.0/jobs')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

if __name__ == '__main__':
    unittest.main()
