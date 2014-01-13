import sys 
sys.path.append('../src')

import httplib
import urllib
import copy
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

def get_json(response, expectedStatus=200):
    if response.status != expectedStatus:
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
    job = {
      'version': '0.1.0',
      'task_id': None,
      'group_id': 'sdfosadflskjdf',
      'command': ['worker-helper.sh', 'run-from-repo', 'hg://...', '456gm4o5g554g', '--', 'run-this-file.py', 'arg_for_run-this-file.py'],
      'tags': {
        'name': 'my-special-task',
        'requester': 'somebody@mozilla.org',
      },
      'parameters': {
        'hardware': {
            'ram': '8G',
            'ami': 'ami-1234567',
            'docker': 'mozilla/build...',
            'hostname': 'specialhost.mozilla.org',
        },
        'software': {
            'os': 'Linux',
            'os_version': 'Slackware 1.5',
        }
      },
      'priority': 0,
      'max_retries': 3,
      'max_runtime_seconds': 7200,
      'max_pending_seconds': 86400,
      'results_server': 'http://treeherder.mozilla.org',
      'data': {},
      'state': 'pending',
      'result': {
        'version': '0.1.0',
        'task_result': {
            'exit_status': 1, 
            'exit_status_string': 'SUCCESS',
        },
        'infra_result': {},
        'extra_info': {
            'aws_instance_id': 'i-1234567890',
            'aws_instance_type': 'm3.xlarge',
            'estimated_cost': 2.0,
        },
        'times': {
            'submitted_timestamp': 0,
            'started_timestamp': 0,
            'finished_timestamp': 0,
            'runtime_seconds': 0
        },
      },
    }

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
   
    def tearDown(self):
        self.conn.close()

    def test_new_job_post(self):
        headers = {"Content-Type": "application/json"}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        try:
            res = get_json(resp)
        except:
            self.assertTrue(False, "failed to parse json response after posting a new job")

        uuid = res['job_uuid']

        self.conn.request('GET', '/0.1.0/jobs?state=PENDING')
        pending = get_json(self.conn.getresponse())

        found = False
        for u in pending:
            if uuid == u:
               found = True
               break
        self.assertTrue(found, "UUID was found in the pending jobs queue")

        # TODO: cleanup job via cancel - is this necessary?

    def test_new_job_post_valid_fields(self):
        badjob = copy.deepcopy(self.job)
        badjob['priority'] = 99
        badjob['max_pending_seconds'] = 604800
        badjob['max_runtime_seconds'] = 86400
        badjob['results_server'] = "http://allizom.com/jobq"
        headers = {"Content-Type": "application/json",
                   "Content-Length": len(json.dumps(badjob))}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(badjob), headers)

        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        try:
            res = get_json(resp)
        except:
            self.assertTrue(False, "failed to parse json response after posting a new job")

        uuid = res['job_uuid']
        self.conn.request('GET', '/0.1.0/job/%s' % uuid)
        job_object = get_json(self.conn.getresponse())

        for field in ['priority', 'max_pending_seconds', 'max_runtime_seconds', 'results_server']:
            self.assertEqual(badjob[field], job_object[field])

        # TODO: cleanup job via cancel - is this necessary?

    def test_new_job_post_valid_types(self):
        badjob = copy.deepcopy(self.job)
        badjob['priority'] = '99'
        badjob['max_runtime_seconds'] = '86400'
        badjob['max_pending_seconds'] = '604800'
        headers = {"Content-Type": "application/json",
                   "Content-Length": len(json.dumps(badjob))}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(badjob), headers)

        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        try:
            res = get_json(resp)
        except:
            self.assertTrue(False, "failed to parse json response after posting a new job")
        uuid = res['job_uuid']
        self.conn.request('GET', '/0.1.0/job/%s' % uuid)
        job_object = get_json(self.conn.getresponse())

        for field in ['priority', 'max_pending_seconds', 'max_runtime_seconds']:
            self.assertTrue(type(job_object[field]) == type(1), "Type of field %s should be int" % field)
            self.assertFalse(badjob[field] == job_object[field], "%s != %s due to mismatched types" % (badjob[field], job_object[field]))

    def test_new_job_post_invalid_priority(self):
        badjob = copy.deepcopy(self.job)
        badjob['priority'] = 9999
        headers = {"Content-Type": "application/json"}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(badjob), headers)

        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        try:
            res = get_json(resp, 405)
        except:
            self.assertTrue(False, "failed to parse json response after posting a new job")
        self.assertTrue(len(res['reason']) > 0, "Job Failed for reason %s" % res['reason'])

    def test_new_job_post_invalid_runtime(self):
        badjob = copy.deepcopy(self.job)
        badjob['max_runtime_seconds'] = 9999999
        headers = {"Content-Type": "application/json"}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(badjob), headers)

        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        try:
            res = get_json(resp, 405)
        except:
            self.assertTrue(False, "failed to parse json response after posting a new job")
        self.assertTrue(len(res['reason']) > 0, "Job Failed for reason %s" % res['reason'])

    def test_new_job_post_invalid_pending(self):
        badjob = copy.deepcopy(self.job)
        badjob['max_pending_seconds'] = 9999999
        headers = {"Content-Type": "application/json"}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(badjob), headers)

        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        try:
            res = get_json(resp, 405)
        except:
            self.assertTrue(False, "failed to parse json response after posting a new job")
        self.assertTrue(len(res['reason']) > 0, "Job Failed for reason %s" % res['reason'])

    def test_new_job_post_invalid_server(self):
        badjob = copy.deepcopy(self.job)
        badjob['results_server'] = 'hack'
        headers = {"Content-Type": "application/json"}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(badjob), headers)

        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

        try:
            res = get_json(resp, 405)
        except:
            self.assertTrue(False, "failed to parse json response after posting a new job")
        self.assertTrue(len(res['reason']) > 0, "Job Failed for reason %s" % res['reason'])

    def test_new_job_post_invalid_json(self):
        headers = {"Content-Type": "application/json"}
        self.conn.request("POST", "/0.1.0/job/new", "hack", headers)

        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

    def test_new_job_get(self):
        headers = {"Content-Type": "application/json"}
        self.conn.request("GET", "/0.1.0/job/new", "hack", headers)

        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 405)

if __name__ == '__main__':
    unittest.main()
