#!/usr/bin/env python
import os
import sys
import time
import Queue
import threading
import subprocess
import shlex
import signal

pipe_name = 'jobs.pipe'
max_jobs = 4
jobs_running = 0
jobs = []
jobs_cv = threading.Condition(threading.Lock())
commands = Queue.Queue()

saturated = threading.Condition(threading.Lock())

def sigchld_handler(signum, frame):
    global jobs_running
    # this means that a subprocess executed,
    # so we decrement the jobs_running variable
    saturated.acquire()
    jobs_running -= 1
    saturated.notify()
    saturated.release()

def run_jobs():
    global jobs_running
    global jobs
    while True:
        saturated.acquire()
        while jobs_running == max_jobs:
            saturated.wait()
        jobs_running += 2 # increment by two since the 'git pull' will decrement
        saturated.release()

        jobs_cv.acquire()
        while len(jobs)==0:
            jobs_cv.wait()
        job = jobs[0]
        jobs = jobs[1:]
        jobs_cv.release()

        subprocess.call(shlex.split('git pull'))
        subprocess.Popen(shlex.split(job))

def handle_commands():
    while True:
        cmd, args = commands.get(block=True)
        if cmd=="job":
            jobs_cv.acquire()
            jobs.append(args)
            jobs_cv.notify()
            jobs_cv.release()
        elif cmd=="ls":
            jobs_cv.acquire()
            print jobs # TODO: find a way to return feedback
            jobs_cv.release()
        else:
            print 'Error: unknown command'

def receive_commands():
    while True:
        try:
            with open(pipe_name, 'r') as pipein:
                command_components = pipein.read().split(':')
                for i in xrange(0,len(command_components)-1,2):
                    commands.put((command_components[i], command_components[i+1]))
        except IOError as e:
            # restart the system call after handling SIGCHILD
            pass

def main():
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

    receive_commands()


if __name__=="__main__":
    main()
