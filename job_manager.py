#!/usr/bin/env python
import os
import sys
import time
import Queue
import threading
import subprocess
import shlex
import signal
import json
import argparse

pipe_name = 'jobs.pipe'
max_jobs = 4
jobs_running = 0

jobs = []
current_job_id = 0
jobs_cv = threading.Condition(threading.Lock())
commands_q = Queue.Queue()

saturated = threading.Condition(threading.Lock())

def sigchld_handler(signum, frame):
    global jobs_running
    # this means that a subprocess executed,
    # so we decrement the jobs_running variable
    saturated.acquire()
    jobs_running -= 1
    saturated.notify()
    saturated.release()

def prehooks(cmd_json):
    global jobs_running

    if cmd_json['git']:
        saturated.acquire()
        jobs_running += 1 # need to increment this because sigchild handler will dec
        saturated.release()
        subprocess.call(shlex.split('git pull'))

    if cmd_json['make']:
        saturated.acquire()
        jobs_running += 1 # need to increment this because sigchild handler will dec
        saturated.release()
        subprocess.call(['make'])

def run_jobs():
    global jobs_running
    global jobs
    while True:
        jobs_cv.acquire()
        while len(jobs)==0:
            jobs_cv.wait()
        job = jobs[0]
        jobs = jobs[1:]
        jobs_cv.release()

        saturated.acquire()
        while jobs_running >= max_jobs:
            saturated.wait()
        jobs_running += 1
        saturated.release()

        subprocess.Popen(job['run'], shell=True)

def handle_submit_job(command):
    global current_job_id
    jobs_cv.acquire()
    jobs.append(command)
    jid = current_job_id
    current_job_id += 1
    jobs_cv.notify()
    jobs_cv.release()
    ret = {'code': 0, 'status': 'OK', 'job_id': jid, 'message': 'job submitted successfully'}
    with open(command['port'], 'w') as f:
        f.write(json.dumps(ret))

def handle_get_listing(command):
    global max_jobs
    global jobs_running
    global jobs
    jobs_cv.acquire()
    queued = str(jobs)
    jobs_cv.release()
    running = jobs_running
    ret = {'code': 0, 'status': 'OK', 'jobs_running': running, 'jobs_queued': queued, 'max_jobs_running': max_jobs}
    with open(command['port'], 'w') as f:
        f.write(json.dumps(ret))

def handle_configure(command):
    global max_jobs
    old_max_jobs = max_jobs
    new_max_jobs = command['max_jobs']
    ret = {'code': 0, 'status': 'OK',
            'old_max_jobs_running': old_max_jobs, 'new_max_jobs_running': new_max_jobs,
            'message': 'configuration successful'}

    if new_max_jobs >= 0:
        saturated.acquire()
        max_jobs = new_max_jobs
        saturated.notify()
        saturated.release()
    else:
        ret['code'] = 2
        ret['status'] = error
        ret['message'] = 'invalid new max jobs running (must be >= 0)'
    with open(command['port'], 'w') as f:
        f.write(json.dumps(ret))

def handle_invalid(command):
    ret = {'code': 1, 'status': 'error', 'message': 'unknown command'}
    with open(command['port'], 'w') as f:
        f.write(json.dumps(ret))

def handle_commands():
    handlers = {'job': handle_submit_job,
                'list': handle_get_listing,
                'configure': handle_configure,
                }
    while True:
        command = commands_q.get(block=True)
        prehooks(command)
        if command['type'] not in handlers:
            handle_invalid(command)
        else:
            handlers[command['type']](command)

def receive_commands_forever():
    while True:
        try:
            with open(pipe_name, 'r') as pipein:
                commands = pipein.read().split('\n')
                for command in commands:
                    if len(command) > 0:
                        commands_q.put(json.loads(command))

        except IOError as e:
            # restart the system call after handling SIGCHILD
            pass

def main(args):
    global pipe_name
    global max_jobs
    pipe_name = args.pipe_name
    max_jobs = args.max_jobs
    signal.signal(signal.SIGCHLD, sigchld_handler)
    try:
        os.mkfifo(pipe_name)
    except Exception:
        sys.stderr.write("Warning: pipe %s already exists.\n" % pipe_name)
        pass

    command_thread = threading.Thread(target=handle_commands)
    command_thread.daemon=True
    command_thread.start()

    job_thread = threading.Thread(target=run_jobs)
    job_thread.daemon=True
    job_thread.start()

    receive_commands_forever()


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Manage job submissions")
    parser.add_argument('--max-jobs-running', dest='max_jobs', type=int, required=True, help="maximum # of jobs to run at any given time (rest are queued)")
    parser.add_argument('--pipe-name', dest='pipe_name', default='jobs.pipe', help="name of named pipe used for job submission")
    args = parser.parse_args()
    main(args)
