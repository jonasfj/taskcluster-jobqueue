import sys 
sys.path.append('../src')

import httplib
import json
import unittest
import subprocess
import threading
import time
from wsgiref.simple_server import make_server
import socket

import jobqueue 

# Gets an open port starting with the seed by incrementing by 1 each time
def find_open_port(ip, seed):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        connected = False
        if isinstance(seed, basestring):
            seed = int(seed)
        maxportnum = seed + 5000 # We will try at most 5000 ports to find an open one
        while not connected:
            try:
                s.bind((ip, seed))
                connected = True
                s.close()
                break
            except:
                if seed > maxportnum:
                    print('Error: Could not find open port after checking 5000 ports')
                    raise
            seed += 1
    except:
        print('Error: Socket error trying to find open port')

    return seed

def get_json(response):
    if response.status != 200:
        print('error: bad http status: %d' % response.status)
        return {}

    text = response.read().strip()

    try:
        decoded = json.loads(text)
    except ValueError:
        print('could not decode: ' + text)
        return {}

    return decoded

#TODO: test worker_id stuff

class TestJobQueueREST(unittest.TestCase):

    # JobQueue server instance running in its own thread
    httpd = None

    @classmethod
    def setUpClass(cls):
        cls.port = find_open_port('127.0.0.1', 15707)
        cls.httpd = make_server('0.0.0.0', cls.port, jobqueue.application)
        thread = threading.Thread(target=cls.httpd.serve_forever)
        thread.daemon = True
        thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def setUp(self):
        self.conn = httplib.HTTPConnection('localhost', TestJobQueueREST.port)

        self.job = {'version': '0.1.0'}
   
    def tearDown(self):
        self.conn.close()

    def test_new_job(self):
        jobs = []
        NUM_JOBS = 10

        # new jobs    
        for i in xrange(0, NUM_JOBS):
            headers = {"Content-Type": "application/json",
                       "Content-Length": len(json.dumps(self.job))}
            self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)

            res = get_json(self.conn.getresponse())
            job = res['job_uuid']
            jobs.append(job)
        self.assertEqual(len(jobs), NUM_JOBS)

        # new jobs should appear in jobs list
        self.conn.request('GET', '/0.1.0/jobs')
        res = get_json(self.conn.getresponse())
        for job in jobs:
            self.assertTrue(job in res)

        # new job should be pending
        a_job = jobs[0]
        self.conn.request('GET', '/0.1.0/job/' + a_job + '/status')
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
        a_job = res['job_uuid']

        # cancel job
        self.conn.request('POST', '/0.1.0/job/' + a_job + '/cancel')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # should be finished
        self.conn.request('GET', '/0.1.0/job/' + a_job + '/status')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['state'], 'FINISHED')

        # should not appear in all jobs
        self.conn.request('GET', '/0.1.0/jobs')
        res = get_json(self.conn.getresponse())
        self.assertTrue(a_job not in res)

        # can't cancel unknown job uuid
        self.conn.request('POST', '/0.1.0/job/-/cancel')
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
        a_job = res['job_uuid']

        # claim
        self.conn.request('POST', '/0.1.0/job/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        our_job = res['job_uuid']

        # cancel job
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/cancel')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # should be finished
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/status')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['state'], 'FINISHED')

        # should not appear in all jobs
        self.conn.request('GET', '/0.1.0/jobs')
        res = get_json(self.conn.getresponse())
        self.assertTrue(our_job not in res)

        # can't cancel unknown job uuid
        self.conn.request('POST', '/0.1.0/job/-/cancel')
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
        a_job = res['job_uuid']

        # claim
        self.conn.request('POST', '/0.1.0/job/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        our_job = res['job_uuid']

        # claimed job should be running
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/status')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['state'], 'RUNNING')

        # should not be in pending list
        self.conn.request('GET', '/0.1.0/jobs?state=PENDING')
        res = get_json(self.conn.getresponse())
        self.assertTrue(our_job not in res)

        # should be in all jobs running list
        self.conn.request('GET', '/0.1.0/jobs?state=RUNNING')
        res = get_json(self.conn.getresponse())
        self.assertTrue(our_job in res)

    def test_job_heartbeat(self):
        # new job
        headers = {"Content-Type": "application/json",
                   "Content-Length": len(json.dumps(self.job))}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        a_job = res['job_uuid']

        # claim
        self.conn.request('POST', '/0.1.0/job/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        our_job = res['job_uuid']

        # heartbeat initially None
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/status')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['last_heartbeat_time'], 'None')

        # heartbeat
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/heartbeat')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # heartbeat has changed
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/status')
        res = get_json(self.conn.getresponse())
        self.assertNotEqual(res['last_heartbeat_time'], 'None')

        # can't complete bad job uuid
        self.conn.request('POST', '/0.1.0/job/-/heartbeat')
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
        a_job = res['job_uuid']

        # claim
        self.conn.request('POST', '/0.1.0/job/claim')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        res = get_json(resp)
        our_job = res['job_uuid']

        # complete
        self.conn.request('POST', '/0.1.0/job/' + our_job + '/complete')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        # should be finished
        self.conn.request('GET', '/0.1.0/job/' + our_job + '/status')
        res = get_json(self.conn.getresponse())
        self.assertEqual(res['state'], 'FINISHED')

        # should no longer be in all jobs running list
        self.conn.request('GET', '/0.1.0/jobs?state=RUNNING')
        res = get_json(self.conn.getresponse())
        self.assertTrue(our_job not in res)

        # can't complete bad job uuid
        self.conn.request('POST', '/0.1.0/job/-/complete')
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
        a_job = res['job_uuid']
        self.conn.request('POST', '/0.1.0/job/' + a_job + '/complete')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 403)

    def test_badmethods(self):
        self.conn.request('GET', '/0.1.0/job/new')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)
        self.conn.request('POST', '/0.1.0/job/00000000-0000-0000-0000-000000000000/status')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        self.conn.request('GET', '/0.1.0/job/00000000-0000-0000-0000-000000000000/cancel')
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        self.conn.request('GET', '/0.1.0/job/claim')
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
