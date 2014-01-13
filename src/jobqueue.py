from urlparse import urlparse
from wsgiref.simple_server import make_server
from wsgiref.util import request_uri
from cgi import parse_qs, escape

import datetime
import heapq
import json
import MySQLdb
import uuid
import re

# TODO: oauth authentication
#       https://github.com/simplegeo/python-oauth2
#       python persona from jonas to get tokens
#       steal two legged oauth from jeads

# TODO: relational database as backing store

# TODO: config file for these
#       add thread / timeout timer to check these values
# queue time limit in seconds
DEFAULT_MAX_PENDING_TIME = 24*60*60
DEFAULT_MAX_RUNNING_TIME = 2*60*60

# TODO: logging

# TODO: heartbeat
#       configure desired heartbeat interval
#       configure max number of missed heartbeats before cancel

class Job(object):

    DEFAULT_PRIORITY = 0

    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    FINISHED = 'FINISHED'

    def __init__(self, priority=DEFAULT_PRIORITY):
        self.uuid = str(uuid.uuid1())
        self.state = Job.PENDING
        self.entered_queue_time = datetime.datetime.now()
        self.started_running_time = None
        self.finished_time = None
        self.last_heartbeat_time = None
        self.missed_heartbeats = 0
        self.job_results = None
        self.worker_id = None

        # fields that we retrive from the incoming job object
        self.max_pending_time = DEFAULT_MAX_PENDING_TIME
        self.max_running_time = DEFAULT_MAX_RUNNING_TIME
        self.priority = priority 
        self.result_graveyard = None
        self.job_object = None

    def get_status_json(self):
        return '{"job_uuid":"%s","state":"%s","last_heartbeat_time":"%s"}' % (self.uuid, self.state, self.last_heartbeat_time)

    def __lt__(self, other):
        if self.priority == other.priority:
            return self.entered_queue_time < other.entered_queue_time
        else:
            return self.priority < other.priority

    def __str__(self):
        return 'job %s' % self.uuid

def make200(start_response, response_body):
    status = "200 OK"
    response_headers = [("Content-Type", "application/json"),
                        ("Content-Length", str(len(response_body)))]
    start_response(status, response_headers)
    return response_body

def make403(start_response):
    status = "403 FORBIDDEN"
    response_body = "Not found"
    response_headers = [("Content-Type", "text/html"),
                        ("Content-Length", str(len(response_body)))]
    start_response(status, response_headers)
    return response_body

def make404(start_response):
    status = "404 NOT FOUND"
    response_body = "Not found"
    response_headers = [("Content-Type", "text/html"),
                        ("Content-Length", str(len(response_body)))]
    start_response(status, response_headers)
    return response_body

def make405(start_response, response_body="{'reason': 'Method not allowed'}"):
    status = "405 METHOD NOT ALLOWED"
    # TODO: require reason to be a json structure?
    response_headers = [("Content-Type", "applicatoin/json"),
                        ("Content-Length", str(len(response_body)))]
    start_response(status, response_headers)
    return response_body

def extract_job_uuid(request):
    uuid = request.path.split('/')[3]
    return uuid

def extract_worker_id(request):
    # TODO:
    return '' 

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
        data = json.loads(environ['wsgi.input'].read(length))
    except:
        return None

    return data

class JobQueue(object):

    def __init__(self):
        # map request handler to request path
        self.urlpatterns = (
            ('/0.1.0/job/new(/)?$', JobQueue.job_new),
            ('/0.1.0/job/[-\w]+(/)?$', JobQueue.job_object),
            ('/0.1.0/job/[-\w]+/status(/)?$', JobQueue.job_status),
            ('/0.1.0/job/[-\w]+/cancel(/)?$', JobQueue.job_cancel),
            ('/0.1.0/job/claim(/)?$', JobQueue.job_claim),
            ('/0.1.0/job/[-\w]+/heartbeat(/)?$', JobQueue.job_heartbeat),
            ('/0.1.0/job/[-\w]+/complete(/)?$', JobQueue.job_complete),
            ('/0.1.0/jobs(/)?$', JobQueue.jobs),
        )

        # TODO: post Q1 change job priority

        # track jobs
        self.pending_queue = [] # heap based priority queue
        self.running_list = [] # array of running jobs
        self.all_jobs = {} # dict containing all jobs

    def dispatch(self, method, start_response, request, environ):
        for pattern, request_handler in self.urlpatterns:
            if re.match(pattern, request.path):
                return request_handler(self, method, start_response, request, environ)
        return make404(start_response)

    def post_results(self, job, results, environ):
        # TODO: retry if post fails?
        #       handle null results - cancelled
        #       treeherder client
        pass

    def add_job_to_pending_queue(self, job):
        heapq.heappush(self.pending_queue, job)

    def remove_job_from_pending_queue(self):
        job = None
        try:
            job = heapq.heappop(self.pending_queue)
        except IndexError:
            pass

        return job

    def job_new(self, method, start_response, request, environ):
        postdata = extract_post_data(environ)
        if not postdata:
            return make405(start_response)

        job = Job()
        job.job_object = postdata

        # these fields are not required, if they exist they will overwrite default
        # we can accept int or string for integer based fields
        if 'priority' in postdata:
            value = int(postdata['priority'])
            if value >= 0 and value <= 99:
                job.priority = value
            else:
                return make405(start_response, '{"reason": "invalid value %s for priority"}' % value)

        if 'max_pending_seconds' in postdata:
            value = int(postdata['max_pending_seconds'])
            # maximum pending is 7 day
            if value >= 0 and value <= 604800:
                job.max_pending_seconds = value
            else:
                return make405(start_response, '{"reason": "invalid value %s for max_pending_seconds"}' % value)

        if 'max_runtime_seconds' in postdata:
            value = int(postdata['max_runtime_seconds'])
            # maximum runtime is 1 day
            if value >= 0 and value <= 86400:
                job.max_runtime_seconds = value
            else:
                return make405(start_response, '{"reason": "invalid value %s for max_runtime_seconds"}' % value)

        if 'results_server' in postdata:
            value = str(postdata['results_server'])
            if value and len(value) > 10:
                job.result_graveyard = value
            else:
                return make405(start_response, '{"reason": "invalid results_server %s"}' % value)

        # populate job object with job status
        job.job_object['task_id'] = job.uuid
        job.job_object['priority'] = job.priority
        job.job_object['max_runtime_seconds'] = job.max_runtime_seconds
        job.job_object['max_pending_seconds'] = job.max_pending_seconds
        job.job_object['results_server'] = job.result_graveyard

        self.add_job_to_pending_queue(job)
        self.all_jobs[job.uuid] = job
        response_body = '{"job_uuid": "' + job.uuid + '"}'
        return make200(start_response, response_body)

    def job_object(self, method, start_response, request, environ):
        if method != 'GET':
            return make405(start_response)

        uuid = extract_job_uuid(request)
        try:
            job = self.all_jobs[uuid]
            return make200(start_response, json.dumps(job.job_object))
        except KeyError:
            return make404(start_response)

    def job_status(self, method, start_response, request, environ):
        if method != 'GET':
            return make405(start_response)

        uuid = extract_job_uuid(request)
        try:
            job = self.all_jobs[uuid]
            return make200(start_response, job.get_status_json())
        except KeyError:
            return make404(start_response)

    def job_cancel(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        uuid = extract_job_uuid(request)
        try:
            job = self.all_jobs[uuid]
        except KeyError:
            return make404(start_response)

        if job.state == Job.PENDING:
            self.pending_queue.remove(job)
            heapq.heapify(self.pending_queue)
            job.state = Job.FINISHED
            # TODO: reason finished
            self.post_results(job, None, environ)
            return make200(start_response, '{}')
        elif job.state == Job.RUNNING:
            self.running_list.remove(job)
            job.state = Job.FINISHED
            # TODO: reason finished
            #       notify worker/provisioner to cancel job
            self.post_results(job, None, environ)
            return make200(start_response, '{}')
        else:
            return make403(start_response)

    def job_claim(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        # TODO: validate worker id?
        worker_id = extract_worker_id(request)

        job = self.remove_job_from_pending_queue()
        if job is None:
            response_body = '{}'
        else:
            job.state = Job.RUNNING
            job.worker_id = worker_id
            # TODO: write this to the job.job_object structure. do we want that?
            self.running_list.append(job)
            response_body = '{"job_uuid": "' + job.uuid + '"}'

        return make200(start_response, response_body)

    def job_heartbeat(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        worker_id = extract_worker_id(request)

        uuid = extract_job_uuid(request)
        try:
            job = self.all_jobs[uuid]
        except:
            return make404(start_response)

        if worker_id == job.worker_id:
            job.last_heartbeat_time = datetime.datetime.now()
            return make200(start_response, '{}')
        else:
            return make403(start_response)

    def job_complete(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        worker_id = extract_worker_id(request)

        uuid = extract_job_uuid(request)
        try:
            job = self.all_jobs[uuid]
        except KeyError:
            return make404(start_response)

        if job not in self.running_list:
            return make403(start_response)

        if worker_id == job.worker_id:
            self.running_list.remove(job)
            job.state = Job.FINISHED
            results = extract_results(request)
            self.post_results(job, results, environ)
 
            return make200(start_response, '{}')
        else:
            return make403(start_response)

    def jobs(self, method, start_response, request, environ):
        if method != 'GET':
            return make405(start_response)

        job_uuids = []

        params = parse_qs(request.query)
        if 'state' in params:
            if params['state'][0] == 'PENDING':
                for job in self.pending_queue:
                    job_uuids.append(job.uuid)
            elif params['state'][0] == 'RUNNING':
                for job in self.running_list:
                    job_uuids.append(job.uuid)
            else:
                return make403(start_response)
        else:
            # everything
            for job in self.pending_queue:
                job_uuids.append(job.uuid)
            for job in self.running_list:
                job_uuids.append(job.uuid)

        # TODO: make sure this generates valid JSON
        response_body = json.dumps(job_uuids)
        return make200(start_response, response_body)

job_queue = JobQueue()

def application(environ, start_response):
    method = environ.get('REQUEST_METHOD', 'GET')

    request = urlparse(request_uri(environ))
    return job_queue.dispatch(method, start_response, request, environ)

if __name__ == '__main__':
    httpd = make_server('0.0.0.0', 8314, application)
    httpd.serve_forever()
