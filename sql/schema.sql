
drop role if exists jobqueue;
create user jobqueue password 'jobqueue';

-- store jobs
create table Job (
    job_id uuid primary key,
    job_object text,
    state varchar(8),
    priority integer,
    max_pending_seconds integer,
    max_runtime_seconds integer,
    entered_queue_time timestamp,
    started_running_time timestamp,
    finished_time timestamp,
    last_heartbeat_time timestamp,
    missed_heartbeats integer,
    worker_id integer,
    job_results text
);
grant all privileges on table Job to jobqueue;

-- what we know about workers
-- TODO: add authentication stuff, etc. 
create table Worker (
    worker_id integer primary key
);
grant all privileges on table Worker to jobqueue;
