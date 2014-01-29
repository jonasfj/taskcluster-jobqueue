from amqplib import client_0_8 as amqp
import sys
sys.path.append('../src')

import http
import urllib
import copy
import json
import psycopg2
import subprocess
import threading
import time
import unittest
from wsgiref.simple_server import make_server

import jobqueue
import util

RABBITMQ_HOST = 'localhost:5672'

def get_json(response, expectedStatus=200):
    if response.status != expectedStatus:
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
        dbpath = 'dbname=jobqueue user=jobqueue host=localhost password=jobqueue'
        dbconn = psycopg2.connect(dbpath)
        cursor = dbconn.cursor()
        cursor.execute('delete from Job');
        cursor.execute('delete from Worker');
        dbconn.commit()

        app = jobqueue.Application(dbpath, RABBITMQ_HOST, '127.0.0.1')

        cls.port = util.find_open_port('127.0.0.1', 15707)
        cls.httpd = make_server('0.0.0.0', cls.port, app)
        thread = threading.Thread(target=cls.httpd.serve_forever)
        thread.daemon = True
        thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def setUp(self):
        self.conn = http.client.HTTPConnection('localhost', TestJobQueueREST.port)

    def tearDown(self):
        self.conn.close()

        # purge queue to get rid of the jobs we created
        rabbit_conn = amqp.Connection(host=RABBITMQ_HOST, userid='guest', password='guest', virtual_host='/', insist=False)
        rabbit_chan = rabbit_conn.channel()
        rabbit_chan.queue_purge(queue='jobs')

    def test_new_job_post(self):
        headers = {"Content-Type": "application/json"}
        self.conn.request("POST", "/0.1.0/job/new", json.dumps(self.job), headers)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)

        try:
            res = get_json(resp)
        except:
            self.assertTrue(False, "failed to parse json response after posting a new job")

        uuid = res['job_id']

        self.conn.request('GET', '/0.1.0/jobs?state=PENDING')
        pending = get_json(self.conn.getresponse())

        found = False
        for u in pending:
            if uuid == u['job_id']:
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

        uuid = res['job_id']
        self.conn.request('GET', '/0.1.0/job/%s' % uuid)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        job_object = json.loads(get_json(resp)['job_object'])

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
        uuid = res['job_id']
        self.conn.request('GET', '/0.1.0/job/%s' % uuid)
        resp = self.conn.getresponse()
        self.assertEqual(resp.status, 200)
        job_object = json.loads(get_json(resp)['job_object'])

        for field in ['priority', 'max_pending_seconds', 'max_runtime_seconds']:
            self.assertTrue(badjob[field] == job_object[field], "%s != %s due to mismatched types" % (badjob[field], job_object[field]))

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
