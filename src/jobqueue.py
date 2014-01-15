import datetime
import json
import os
import re
import sqlite3
import uuid
import urllib

from wsgiref.simple_server import make_server
from wsgiref.util import request_uri

# TODO: oauth authentication
#       https://github.com/simplegeo/python-oauth2
#       python persona from jonas to get tokens
#       steal two legged oauth from jeads

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

    @staticmethod
    def locate(job_id, dbpath):
        query = """select job_id, job_object, state, priority,
                          max_pending_seconds, max_runtime_seconds,
                          entered_queue_time, started_running_time,
                          finished_time, last_heartbeat_time,
                          missed_heartbeats, worker_id, job_results
                   from Job where job_id=:job_id"""

        conn = sqlite3.connect(dbpath)
        cursor = conn.cursor()
        cursor.execute(query, {'job_id': job_id})
        row = cursor.fetchone()
        if row is None:
            return None

        job = Job()
        job.dbpath = dbpath
        job.job_id = row[0]
        job.job_object = row[1]
        job.state = row[2]
        job.priority = int(row[3])
        job.max_pending_seconds = row[4]
        job.max_runtime_seconds = row[5]
        job.entered_queue_time = row[6]
        job.started_running_time = row[7]
        job.finished_time = row[8]
        job.last_heartbeat_time = row[9]
        job.missed_heartbeats = row[10]
        job.worker_id = row[11]
        job.job_results = row[12]

        return job

    @staticmethod
    def locate_all(dbpath, state=None):
        conn = sqlite3.connect(dbpath)
        cursor = conn.cursor()
        if state is not None:
            query = """select job_id, job_object, state, priority,
                              max_pending_seconds, max_runtime_seconds,
                              entered_queue_time, started_running_time,
                              finished_time, last_heartbeat_time,
                              missed_heartbeats, worker_id, job_results
                       from Job where state=:state"""
            cursor.execute(query, {'state': state})
        else:
            query = """select job_id, job_object, state, priority,
                              max_pending_seconds, max_runtime_seconds,
                              entered_queue_time, started_running_time,
                              finished_time, last_heartbeat_time,
                              missed_heartbeats, worker_id, job_results
                       from Job"""
            cursor.execute(query)

        jobs = []
        rows = cursor.fetchmany()
        while len(rows) > 0:
            for row in rows:
                job = Job()
                job.dbpath = dbpath
                job.job_id = row[0]
                job.job_object = row[1]
                job.state = row[2]
                job.priority = int(row[3])
                job.max_pending_seconds = row[4]
                job.max_runtime_seconds = row[5]
                job.entered_queue_time = row[6]
                job.started_running_time = row[7]
                job.finished_time = row[8]
                job.last_heartbeat_time = row[9]
                job.missed_heartbeats = row[10]
                job.worker_id = row[11]
                job.job_results = row[12]
                jobs.append(job)
            rows = cursor.fetchmany()

        return jobs

    def __init__(self, dbpath=None, job_object={}):

        # database connection
        self.dbpath = dbpath

        self.job_id = str(uuid.uuid1())
        self.state = None 
        self.entered_queue_time = None 
        self.started_running_time = None
        self.finished_time = None
        self.last_heartbeat_time = None
        self.missed_heartbeats = 0
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
        if dbpath is not None:
            query = """
                    insert into Job(job_id,job_object,state,priority,max_pending_seconds,max_runtime_seconds,missed_heartbeats)
                    values(?,?,?,?,?,?,?)
                    """

            conn = sqlite3.connect(dbpath)
            cursor = conn.cursor()
            cursor.execute(query, (self.job_id, self.job_object, self.state,
                                   self.priority, self.max_pending_seconds,
                                   self.max_runtime_seconds, self.missed_heartbeats))
            conn.commit()

        #TODO result graveyard

    def get_status_json(self):
        return '{"job_id":"%s","state":"%s","last_heartbeat_time":"%s"}' % (str(self.job_id), self.state, self.last_heartbeat_time)

    def finish(self, result=None):
        self.state = Job.FINISHED
        self.job_results = result

        # TODO: finished time
        if self.dbpath:
            conn = sqlite3.connect(self.dbpath)
            cursor = conn.cursor()
            query = 'update Job set state=?,job_results=? where job_id=?'
            cursor.execute(query, (self.state, self.job_results, self.job_id))
            conn.commit()
            conn.close()


    def heartbeat(self):
        self.last_heartbeat_time = datetime.datetime.now()

        if self.dbpath:
            query = 'update Job set last_heartbeat_time=? where job_id=?'

            conn = sqlite3.connect(self.dbpath)
            cursor = conn.cursor()
            cursor.execute(query, (self.last_heartbeat_time, self.job_id))
            conn.commit()
            conn.close()

    def pending(self):
        self.state = Job.PENDING
        self.entered_queue_time = datetime.datetime.now()

        if self.dbpath:
            conn = sqlite3.connect(self.dbpath)
            cursor = conn.cursor()
            query = 'update Job set state=?,entered_queue_time=? where job_id=?'
            cursor.execute(query, (self.state, self.entered_queue_time, self.job_id))
            conn.commit()
            conn.close()


    def run(self, worker_id):
        self.state = Job.RUNNING
        self.worker_id = worker_id

        if self.dbpath:
            conn = sqlite3.connect(self.dbpath)
            cursor = conn.cursor()
            query = 'update Job set state=?,worker_id=? where job_id=?'
            cursor.execute(query, (self.state, self.worker_id, self.job_id))
            conn.commit()
            conn.close()

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
    return job_id

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
        data = json.loads(environ['wsgi.input'].read(length).decode())
    except:
        return None

    return data

class JobQueue(object):
    def __init__(self, dbpath):
        # map request handler to request path
        self.urlpatterns = (
            ('/0.1.0/job/new(/)?$', JobQueue.job_new),
            ('/0.1.0/job/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}(/)?$', JobQueue.job_object),
            ('/0.1.0/job/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/status(/)?$', JobQueue.job_status),
            ('/0.1.0/job/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/cancel(/)?$', JobQueue.job_cancel),
            ('/0.1.0/job/claim(/)?$', JobQueue.job_claim),
            ('/0.1.0/job/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/heartbeat(/)?$', JobQueue.job_heartbeat),
            ('/0.1.0/job/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/complete(/)?$', JobQueue.job_complete),
            ('/0.1.0/jobs(/)?$', JobQueue.jobs),
        )

        # TODO: post Q1 change job priority

        # database
        self.dbpath = dbpath

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

    def add_job_to_pending_queue(self, job):
        conn = sqlite3.connect(self.dbpath)
        cursor = conn.cursor()
        conn.execute('insert into JobQueueJob values(?,?,?)', (job.job_id, job.priority, job.entered_queue_time))
        conn.commit()

    def pop_job_from_pending_queue(self):
        job = None

        conn = sqlite3.connect(self.dbpath)
        cursor = conn.cursor()
        cursor.execute('select job_id, min(entered_queue_time) from JobQueueJob where priority=(select min(priority) from JobQueueJob)')
        row = cursor.fetchone()
        if row is not None:
            job_id = row[0]
            job = Job.locate(job_id, self.dbpath)
            if job:
                self.remove_job_from_pending_queue(job)

        return job

    def remove_job_from_pending_queue(self, job):
        conn = sqlite3.connect(self.dbpath)
        cursor = conn.cursor()
        conn.execute('delete from JobQueueJob where job_id=:job_id', {'job_id': job.job_id}) 
        conn.commit()
        pass

    def job_new(self, method, start_response, request, environ):
        job_object = extract_post_data(environ)
        if not job_object:
            return make405(start_response)
        error_response = self.validate_job_object(start_response, job_object)
        if error_response:
            return error_response

        job = Job(self.dbpath, job_object)
        job.pending()
        self.add_job_to_pending_queue(job)
        response_body = '{"job_id": "' + job.job_id + '"}'
        return make200(start_response, response_body)

    def job_object(self, method, start_response, request, environ):
        if method != 'GET':
            return make405(start_response)

        job_id = extract_job_id(request)
        job = Job.locate(job_id, self.dbpath)
        if job:
            return make200(start_response, job.job_object)
        else:
            return make404(start_response)

    def job_status(self, method, start_response, request, environ):
        if method != 'GET':
            return make405(start_response)

        job_id = extract_job_id(request)
        job = Job.locate(job_id, self.dbpath)
        if job:
            return make200(start_response, job.get_status_json())
        else:
            return make404(start_response)

    def job_cancel(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        job_id = extract_job_id(request)
        job = Job.locate(job_id, self.dbpath)
        if job is None:        
            return make404(start_response)

        if job.state == Job.PENDING:
            self.remove_job_from_pending_queue(job)
            job.finish()
            # TODO: reason finished
            self.post_results(job, None, environ)
            return make200(start_response, '{}')
        elif job.state == Job.RUNNING:
            job.finish()
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

        job = self.pop_job_from_pending_queue()
        if job is None:
            response_body = '{}'
        else:
            job.run(worker_id)
            # TODO: write this to the job.job_object structure. do we want that?
            response_body = '{"job_id": "' + job.job_id + '"}'

        return make200(start_response, response_body)

    def job_heartbeat(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        worker_id = extract_worker_id(request)

        job_id = extract_job_id(request)
        job = Job.locate(job_id, self.dbpath)
        if job is None:
            return make404(start_response)

        if worker_id == job.worker_id:
            job.heartbeat()
            return make200(start_response, '{}')
        else:
            return make403(start_response)

    def job_complete(self, method, start_response, request, environ):
        if method != 'POST':
            return make405(start_response)

        worker_id = extract_worker_id(request)

        job_id = extract_job_id(request)
        job = Job.locate(job_id, self.dbpath)
        if job is None:
            return make404(start_response)

        if job.state != Job.RUNNING:
            return make403(start_response)

        if worker_id == job.worker_id:
            results = extract_results(request)
            job.finish(results)
            self.post_results(job, results, environ)
 
            return make200(start_response, '{}')
        else:
            return make403(start_response)

    def jobs(self, method, start_response, request, environ):
        if method != 'GET':
            return make405(start_response)

        job_list = []

        params = urllib.parse.parse_qs(request.query)
        if 'state' in params:
            state = params['state'][0]
            if state not in [Job.PENDING, Job.RUNNING]:
                return make403(start_response)

            job_list = Job.locate_all(self.dbpath, state)
        else:
            job_list = Job.locate_all(self.dbpath)

        # TODO: make sure this generates valid JSON
        response_body = json.dumps([job.__dict__ for job in job_list])
        return make200(start_response, response_body)

class Application(object):
    def __init__(self, dbpath):
        # if not specified, try to find db in some default locations
        if dbpath is None:
            paths = ['jobqueue.db', 'sql/jobqueue.db', '../sql/jobqueue.db']
            for path in paths:
                if os.path.isfile(path):
                    dbpath = path
                    break 

        self.job_queue = JobQueue(dbpath) 

    def __call__(self, environ, start_response):
        method = environ.get('REQUEST_METHOD', 'GET')

        request = urllib.parse.urlparse(request_uri(environ))
        return self.job_queue.dispatch(method, start_response, request, environ)

if __name__ == '__main__':
    app = Application(None)
    httpd = make_server('0.0.0.0', 8314, app)
    httpd.serve_forever()
