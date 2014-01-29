from amqplib import client_0_8 as amqp
from datetime import datetime
import json
import os
import psycopg2
import psycopg2.extras
import re
import sys
import uuid
import urllib
import argparse

from wsgiref.simple_server import make_server
from wsgiref.util import request_uri

# TODO: oauth authentication
#       https://github.com/simplegeo/python-oauth2
#       python persona from jonas to get tokens
#       steal two legged oauth from jeads

# TODO: config file for these
#       add thread / timeout timer to check these values
#       pending time can be enforced by rabbit, but I believe
#       max running time needs to be enforced by the job queue
# queue time limit in seconds
DEFAULT_MAX_PENDING_TIME = 24*60*60
DEFAULT_MAX_RUNNING_TIME = 2*60*60

# TODO: logging

#
psycopg2.extras.register_uuid()

# extract relevant fields from sqlite row
def extract_job_from_row(job, row):
    job.job_id = row[0]
    job.job_object = row[1]
    job.state = row[2]
    job.priority = int(row[3])
    job.max_pending_seconds = int(row[4])
    job.max_runtime_seconds = int(row[5])
    job.entered_queue_time = row[6]
    job.started_running_time = row[7]
    job.finished_time = row[8]
    job.worker_id = row[9]
    job.job_results = row[10]

# only convert datetime to string if field is not None
def datetime_str(dt):
    if dt is not None:
        return str(dt)
    else:
        return dt

class Job(object):

    DEFAULT_PRIORITY = 0

    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    FINISHED = 'FINISHED'

    @staticmethod
    def locate(job_id, dbconn):
        query = """select job_id, job_object, state, priority,
                          max_pending_seconds, max_runtime_seconds,
                          entered_queue_time, started_running_time,
                          finished_time, worker_id, job_results
                   from Job where job_id=%(job_id)s"""

        cursor = dbconn.cursor()
        cursor.execute(query, {'job_id': job_id})
        row = cursor.fetchone()
        if row is None:
            return None

        job = Job()
        job.dbconn = dbconn
        extract_job_from_row(job, row)
        return job

    @staticmethod
    def locate_all(dbconn, state=None):
        cursor = dbconn.cursor()
        if state is not None:
            query = """select job_id, job_object, state, priority,
                              max_pending_seconds, max_runtime_seconds,
                              entered_queue_time, started_running_time,
                              finished_time, worker_id, job_results
                       from Job where state=%(state)s"""
            cursor.execute(query, {'state': state})
        else:
            query = """select job_id, job_object, state, priority,
                              max_pending_seconds, max_runtime_seconds,
                              entered_queue_time, started_running_time,
                              finished_time, worker_id, job_results
                       from Job where state <> 'FINISHED'"""
            cursor.execute(query)

        jobs = []
        rows = cursor.fetchmany()
        while len(rows) > 0:
            for row in rows:
                job = Job()
                job.dbconn = dbconn
                extract_job_from_row(job, row)
                jobs.append(job)
            rows = cursor.fetchmany()

        return jobs

    def __init__(self, dbconn=None, job_object={}):
        self.job_id = uuid.uuid1()
        self.state = None
        self.entered_queue_time = None
        self.started_running_time = None
        self.finished_time = None
        self.job_results = None
        self.worker_id = None

        # fields that we retrieve from the incoming job object
        self.job_object = json.dumps(job_object)

        self.max_pending_seconds = int(job_object.get('max_pending_seconds', DEFAULT_MAX_PENDING_TIME))
        self.max_runtime_seconds = int(job_object.get('max_runtime_seconds', DEFAULT_MAX_RUNNING_TIME))
        self.priority = int(job_object.get('priority', Job.DEFAULT_PRIORITY))
        self.result_graveyard = job_object.get('results_server', '')

        # TODO: these should go into results, leaving job_object with
        #       what we started with
        #job.job_object['job_id'] = job.job_id
        #job.job_object['priority'] = job.priority
        #job.job_object['max_runtime_seconds'] = job.max_runtime_seconds
        #job.job_object['max_pending_seconds'] = job.max_pending_seconds
        #job.job_object['results_server'] = job.result_graveyard

        # update database
        if dbconn is not None:
            query = """
                    insert into Job(job_id,job_object,state,priority,max_pending_seconds,max_runtime_seconds)
                    values(%s,%s,%s,%s,%s,%s)
                    """

            cursor = dbconn.cursor()
            cursor.execute(query, (self.job_id,
                                   self.job_object, self.state,
                                   self.priority, self.max_pending_seconds,
                                   self.max_runtime_seconds))
            dbconn.commit()

        #TODO result graveyard

    def get_json(self):
        return self.job_object
        job_dict = {}
        job_dict['job_id'] = str(self.job_id)
        job_dict['job_object'] = self.job_object
        job_dict['state'] = self.state
        job_dict['priority'] = self.priority
        job_dict['max_pending_seconds'] = self.max_pending_seconds
        job_dict['max_runtime_seconds'] = self.max_runtime_seconds
        job_dict['entered_queue_time'] = datetime_str(self.entered_queue_time)
        job_dict['started_running_time'] = datetime_str(self.started_running_time)
        job_dict['finished_time'] = datetime_str(self.finished_time)
        job_dict['worker_id'] = self.worker_id
        job_dict['job_results'] = self.job_results
        return json.dumps(job_dict)

    def finish(self, dbconn, result=None):
        self.state = Job.FINISHED
        self.job_results = result

        # TODO: finished time
        if dbconn:
            cursor = dbconn.cursor()
            query = 'update Job set state=%s,job_results=%s where job_id=%s'
            cursor.execute(query, (self.state, self.job_results, self.job_id))
            dbconn.commit()

    def pending(self, dbconn):
        self.state = Job.PENDING
        self.entered_queue_time = datetime.now()

        if dbconn:
            cursor = dbconn.cursor()
            query = 'update Job set state=%s,entered_queue_time=%s where job_id=%s'
            cursor.execute(query, (self.state, self.entered_queue_time, self.job_id))
            dbconn.commit()

    def run(self, dbconn, worker_id):
        self.state = Job.RUNNING
        self.worker_id = worker_id

        if dbconn:
            cursor = dbconn.cursor()
            query = 'update Job set state=%s,worker_id=%s where job_id=%s'
            cursor.execute(query, (self.state, self.worker_id, self.job_id))
            dbconn.commit()

    def __lt__(self, other):
        if self.priority == other.priority:
            return self.entered_queue_time < other.entered_queue_time
        else:
            return self.priority < other.priority

    def __str__(self):
        return 'job %s' % str(self.job_id)

def make200(start_response, response_body):
    status = "200 OK"
    response_headers = [("Content-Type", "application/json"),
                        ("Content-Length", str(len(response_body)))]
    start_response(status, response_headers)
    return [response_body.encode('utf-8')]

def make403(start_response):
    status = "403 FORBIDDEN"
    response_body = "Not found"
    response_headers = [("Content-Type", "text/html"),
                        ("Content-Length", str(len(response_body)))]
    start_response(status, response_headers)
    return [response_body.encode('utf-8')]

def make404(start_response):
    status = "404 NOT FOUND"
    response_body = "Not found"
    response_headers = [("Content-Type", "text/html"),
                        ("Content-Length", str(len(response_body)))]
    start_response(status, response_headers)
    return [response_body.encode('utf-8')]

def make405(start_response, response_body="{'reason': 'Method not allowed'}"):
    status = "405 METHOD NOT ALLOWED"
    # TODO: require reason to be a json structure?
    response_headers = [("Content-Type", "applicatoin/json"),
                        ("Content-Length", str(len(response_body)))]
    start_response(status, response_headers)
    return [response_body.encode('utf-8')]

def extract_job_id(request):
    job_id = request.path.split('/')[3]
    return uuid.UUID(job_id)

def extract_worker_id(request):
    # TODO:
    return 0

def extract_results(request):
    # TODO:
    return ''

def extract_post_data(environ):
    #TODO: do we need more robust code here?
    try:
        length = int(environ.get('CONTENT_LENGTH', '0'))
    except ValueError:
        length = 0

    try:
        data = json.loads(environ['wsgi.input'].read(length).decode())
    except:
        return None

    return data

class JobQueue(object):
    def __init__(self, dsn, rabbitmq_host, external_addr):
        # map request handler to request path
        self.urlpatterns = (
            ('/0.1.0/job/new(/)?$', JobQueue.job_new),
            ('/0.1.0/job/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}(/)?$', JobQueue.job_object),
            ('/0.1.0/job/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/cancel(/)?$', JobQueue.job_cancel),
            ('/0.1.0/job/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/claim(/)?$', JobQueue.job_claim),
            ('/0.1.0/job/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/complete(/)?$', JobQueue.job_complete),
            ('/0.1.0/jobs(/)?$', JobQueue.jobs),
        )

        # database
        # TODO: validate dsn?
        self.dsn = dsn

        # rabbitmq
        # TODO: add args for other rabbitmq connection parameters
        self.rabbit_conn = amqp.Connection(host=rabbitmq_host, userid="guest", password="guest", virtual_host="/", insist=False)
        self.rabbit_chan = self.rabbit_conn.channel()

        # TODO: support multiple queues, defined somewhere else
        self.rabbit_chan.queue_declare(queue="jobs", durable=False, exclusive=False, auto_delete=False)
        self.rabbit_chan.exchange_declare(exchange="jobs_xchg", type="direct", durable=False, auto_delete=False)
        self.rabbit_chan.queue_bind(queue="jobs", exchange="jobs_xchg", routing_key="jobs")

        self.external_addr = external_addr

    def dispatch(self, method, start_response, request, environ):
        for pattern, request_handler in self.urlpatterns:
            if re.match(pattern, request.path):
                return request_handler(self, method, start_response, request, environ)
        return make404(start_response)

    def validate_job_object(self, start_response, job_object):
        # these fields are currently not required
        # if they exist they will overwrite default
        # we can accept int or string for integer based fields
        if 'priority' in job_object:
            value = int(job_object['priority'])
            if not (value >= 0 and value <= 99):
                return make405(start_response, '{"reason": "invalid value %s for priority"}' % value)

        if 'max_pending_seconds' in job_object:
            value = int(job_object['max_pending_seconds'])
            # maximum pending is 7 day
            if not (value >= 0 and value <= 604800):
                return make405(start_response, '{"reason": "invalid value %s for max_pending_seconds"}' % value)

        if 'max_runtime_seconds' in job_object:
            value = int(job_object['max_runtime_seconds'])
            # maximum runtime is 1 day
            if not (value >= 0 and value <= 86400):
                return make405(start_response, '{"reason": "invalid value %s for max_runtime_seconds"}' % value)

        if 'results_server' in job_object:
            value = str(job_object['results_server'])
            if not (value and len(value) > 10):
                return make405(start_response, '{"reason": "invalid results_server %s"}' % value)

        return None


    def post_results(self, job, results, environ):
        # TODO: retry if post fails?
        #       handle null results - cancelled
        #       treeherder client
        pass

    def job_new(self, method, start_response, request, environ):
        job_object = extract_post_data(environ)
        if not job_object:
            return make405(start_response)
        error_response = self.validate_job_object(start_response, job_object)
        if error_response:
            return error_response

        dbconn = psycopg2.connect(self.dsn)
        job = Job(dbconn, job_object)
        job.pending(dbconn)

        # add to message queue
        msg_dict = {'job': json.loads(job.get_json()),
                    'claim': "http://{}/0.1.0/{}/claim".format(self.external_addr, job.job_id),
                    'finish': "http://{}/0.1.0/{}/finish".format(self.external_addr, job.job_id)}

        msg = amqp.Message(json.dumps(msg_dict))
        msg.expiration = job.max_pending_seconds*1000  #milliseconds

        self.rabbit_chan.basic_publish(msg, exchange="jobs_xchg", routing_key="jobs")

        response_body = '{"job_id": "' + str(job.job_id) + '"}'
        return make200(start_response, response_body)

    def job_object(self, method, start_response, request, environ):
        if method != 'GET':
            return make405(start_response)

        dbconn = psycopg2.connect(self.dsn)

        job_id = extract_job_id(request)
        job = Job.locate(job_id, dbconn)
        if job:
            return make200(start_response, job.get_json())
        else:
            return make404(start_response)

    def job_cancel(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        job_id = extract_job_id(request)
        dbconn = psycopg2.connect(self.dsn)
        job = Job.locate(job_id, dbconn)
        if job is None:
            return make404(start_response)

        if job.state == Job.PENDING or job.state == Job.RUNNING:
            job.finish(dbconn)
            # TODO: reason finished
            #       if running, notify worker/provisioner to cancel job
            self.post_results(job, None, environ)
            return make200(start_response, '{}')
        else:
            return make403(start_response)

    def job_claim(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        # TODO: validate worker id?
        worker_id = extract_worker_id(request)
        job_id = extract_job_id(request)

        dbconn = psycopg2.connect(self.dsn)
        job = Job.locate(job_id, dbconn)
        if job is None:
            return make404(start_response)

        if job.state == Job.PENDING:
            job.run(dbconn, worker_id)
            return make200(start_response, '{}')

    def job_complete(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        dbconn = psycopg2.connect(self.dsn)

        worker_id = extract_worker_id(request)
        job_id = extract_job_id(request)
        job = Job.locate(job_id, dbconn)
        if job is None:
            return make404(start_response)

        if job.state != Job.RUNNING:
            return make403(start_response)

        if worker_id == job.worker_id:
            results = extract_results(request)
            job.finish(dbconn, results)
            self.post_results(job, results, environ)

            return make200(start_response, '{}')
        else:
            return make403(start_response)

    def jobs(self, method, start_response, request, environ):
        if method != 'GET':
            return make405(start_response)

        job_list = []

        params = urllib.parse.parse_qs(request.query)

        dbconn = psycopg2.connect(self.dsn)
        if 'state' in params:
            state = params['state'][0]
            if state not in [Job.PENDING, Job.RUNNING]:
                return make403(start_response)

            job_list = Job.locate_all(dbconn, state)
        else:
            job_list = Job.locate_all(dbconn)

        # TODO: make sure this generates valid JSON
        response_body = '[' + ','.join([job.get_json() for job in job_list]) + ']'
        return make200(start_response, response_body)

class Application(object):
    def __init__(self, dsn, rabbitmq_host, external_addr):
        self.job_queue = JobQueue(dsn, rabbitmq_host, external_addr)
        print('jobqueue running...')

    def __call__(self, environ, start_response):
        method = environ.get('REQUEST_METHOD', 'GET')

        request = urllib.parse.urlparse(request_uri(environ))
        return self.job_queue.dispatch(method, start_response, request, environ)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dsn', default='dbname=jobqueue user=jobqueue host=localhost password=jobqueue',
                        help="Postgresql DSN connection string")
    parser.add_argument('--external-addr', default='127.0.0.1',
                        help="Externally accessible ip address")
    parser.add_argument('--port', type=int, default=8314,
                        help="Port on which to listen")
    parser.add_argument('--rabbitmq-host', default='127.0.0.1:5672',
                        help="Externally accessible ip address")
    args = parser.parse_args()

    app = Application(args.dsn, args.rabbitmq_host, args.external_addr)
    httpd = make_server('0.0.0.0', args.port, app)
    httpd.serve_forever()

if __name__ == '__main__':
    main()
