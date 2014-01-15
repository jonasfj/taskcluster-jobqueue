
-- store jobs
create table Job (
    job_id text primary key,
    job_object text,
    state text,
    priority integer,
    max_pending_seconds integer,
    max_runtime_seconds integer,
    entered_queue_time integer,
    started_running_time integer,
    finished_time integer,
    last_heartbeat_time integer,
    missed_heartbeats integer,
    worker_id integer,
    job_results text
);

-- store jobs inside the job queue
create table JobQueueJob (
    job_id text primary key,
    priority integer,                 --duplicated from Job to avoid join
    entered_queue_time integer        --duplicated from Job to avoid join
);

-- what we know about workers
-- TODO: add authentication stuff, etc. 
create table Worker (
    worker_id integer primary key
);
