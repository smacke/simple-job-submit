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

all_patts = ['all', '*']

# TODO: all this stuff should be wrapped in some kind of state object and passed around
pipe_name = 'jobs.pipe'
max_jobs = 4
jobs_running = 0 # TODO: rename to num_jobs_running

jobs_q = []
jobs_cv = threading.Condition(threading.Lock())

running_jobs_table = {} # map pid -> (job_id, command)
running_cv = threading.Condition(threading.Lock())

current_job_id = 0
commands_q = Queue.Queue()

saturated = threading.Condition(threading.Lock())

# ref: http://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid
def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True

def sigchld_handler(signum, frame):
    global jobs_running
    # this means that a subprocess executed,
    # so we decrement the jobs_running variable
    todelete = []
    saturated.acquire()
    # this avoids race where multiple SIGCHLDs are delivered --
    # since we only execute one handler for potentially multiple
    # signals, we need to check all running jobs to see whether
    # they are still around
    for pid in running_jobs_table:
        if not check_pid(pid):
            todelete.add(pid)
    for pid in todelete:
        del running_jobs_table[pid]
    jobs_running = len(running_jobs_table)
    saturated.notify()
    saturated.release()

def prehooks(cmd_json):
    if cmd_json['git']:
        subprocess.call(shlex.split('git pull'))

    if cmd_json['make']:
        subprocess.call(['make'])

def run_jobs():
    global jobs_running
    global running_jobs_table
    global jobs_q
    while True:
        saturated.acquire()
        while jobs_running >= max_jobs:
            saturated.wait()
        saturated.release()
        # wait until we actually get a job off the queue
        # before we increment jobs_running

        jobs_cv.acquire()
        while len(jobs_q)==0:
            jobs_cv.wait()

        if jobs_running >= max_jobs:
            # edge case -- a config command could have come in
            # setting max jobs smaller, in which case we need
            # to respect that
            jobs_cv.release()
            continue

        job = jobs_q[0]
        jobs_q = jobs_q[1:]
        jobs_cv.release()

        saturated.acquire()
        proc = subprocess.Popen(job['job'], shell=True)
        running_jobs_table[proc.pid] = job
        jobs_running = len(running_jobs_table)
        saturated.release()
        time.sleep(1.) # sleep a bit in case jobs have sequential dependencies


def handle_submit_job(command):
    global current_job_id
    jobs_cv.acquire()
    jid = current_job_id
    jobs_q.append({'job': command['run'], 'job_id': jid})
    current_job_id += 1
    jobs_cv.notify()
    jobs_cv.release()
    ret = {'code': 0, 'status': 'OK', 'job_id': jid, 'message': 'job submitted successfully'}
    with open(command['port'], 'w') as f:
        f.write(json.dumps(ret))

def handle_stat(command):
    global max_jobs
    global jobs_running
    global running_jobs_table
    global jobs_q
    jobs_cv.acquire()
    queued = str(jobs_q)
    num_queued = len(jobs_q)
    # prevents jobs from showing up in both job queue and as running
    saturated.acquire()
    jobs_running_list = list(running_jobs_table.values())
    saturated.release()
    jobs_cv.release()
    num_running = jobs_running
    ret = {'code': 0, 'status': 'OK', 'jobs_running': jobs_running_list,
            'num_jobs_running': num_running, 'num_jobs_queued': num_queued,
            'jobs_queued': queued, 'max_jobs_running': max_jobs}
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
        ret['status'] = 'error'
        ret['message'] = 'invalid new max jobs running (must be >= 0)'
    with open(command['port'], 'w') as f:
        f.write(json.dumps(ret))

def handle_cancel(command):
    global jobs_q
    cancel_id = command['job_to_cancel']
    jobs_cv.acquire()
    if cancel_id in all_patts:
        success = True
        jobs_cancelled = jobs_q
        jobs_q = []
    else:
        success = False
        for i, job in enumerate(jobs_q):
            if job['job_id'] == cancel_id:
                jobs_cancelled = [job]
                jobs_q = jobs_q[:i] + jobs_q[i+1:]
                success = True
                break
    jobs_cv.release()

    if success:
        ret = {'code': 0, 'status': 'OK', 'jobs_cancelled': jobs_cancelled}
    else:
        ret = {'code': 3, 'status': 'error', 'requested_job_to_cancel': cancel_id, 'message': 'requested cancellation not found in queue'}
    with open(command['port'], 'w') as f:
        f.write(json.dumps(ret))

def handle_shutdown(command):
    global jobs_q
    global jobs_running
    global shutdown_requested
    jobs_cv.acquire()
    jobs_queued = len(jobs_q)
    jobs_cv.release()
    if jobs_running > 0 or jobs_queued > 0:
        do_shutdown = False
        ret = {'code': 4, 'status': 'error', 'num_jobs_running': jobs_running,
                'num_jobs_queued': jobs_queued,
                'message': 'refusing shutdown (jobs still running or in queue)'}
    else:
        do_shutdown = True
        ret = {'code': 0, 'status': 'OK', 'message': 'shutdown successful'}
    with open(command['port'], 'w') as f:
        f.write(json.dumps(ret))
    if do_shutdown:
        with open(pipe_name, 'w') as pipein:
            pipein.write(json.dumps({'SHUTDOWN': True}))

def handle_invalid(command):
    ret = {'code': 1, 'status': 'error', 'message': 'unknown command'}
    with open(command['port'], 'w') as f:
        f.write(json.dumps(ret))

def handle_commands():
    handlers = {'submit_job': handle_submit_job,
                'stat': handle_stat,
                'configure': handle_configure,
                'cancel': handle_cancel,
                'shutdown': handle_shutdown,
                }
    while True:
        command = commands_q.get(block=True)
        prehooks(command)
        if command['type'] not in handlers:
            handle_invalid(command)
        else:
            handlers[command['type']](command)

def receive_commands_forever():
    shutdown_requested = False
    while not shutdown_requested:
        try:
            with open(pipe_name, 'r') as pipein:
                commands = pipein.read().split('\n')
                for command in commands:
                    if len(command) > 0:
                        command = json.loads(command)
                        if 'SHUTDOWN' in command and command['SHUTDOWN']:
                            shutdown_requested = True
                        else:
                            commands_q.put(command)

        except IOError as e:
            # TODO: this should technically check EINTR
            # restart the system call after handling SIGCHILD
            pass

def main(args):
    global pipe_name
    global max_jobs
    pipe_name = args.pipe_name
    max_jobs = args.max_jobs
    signal.signal(signal.SIGCHLD, sigchld_handler)
    os.mkfifo(pipe_name) # if this raises an exception, something is wrong and we should die

    command_thread = threading.Thread(target=handle_commands)
    command_thread.daemon=True
    command_thread.start()

    job_thread = threading.Thread(target=run_jobs)
    job_thread.daemon=True
    job_thread.start()

    try:
        receive_commands_forever()
    except KeyboardInterrupt:
        # TODO: should flush job queue to disk, this kills it with prejudice
        pass
    os.remove(args.pipe_name) # this signals that no manager is running


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Manage job submissions")
    parser.add_argument('--max-jobs-running', dest='max_jobs', type=int, required=True, help="maximum # of jobs to run at any given time (rest are queued)")
    parser.add_argument('--pipe-name', dest='pipe_name', default='jobs.pipe', help="name of named pipe used for job submission")
    args = parser.parse_args()
    main(args)
