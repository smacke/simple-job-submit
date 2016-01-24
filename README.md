Simple Job Submission System
============================

The idea is that you have a bunch of machines to which you have ssh access,
and you have a bunch of computing tasks to perform. For example, you want to
tune hyperparameters on a learning algorithm. You want to be able to submit jobs
and have them queued automatically and run whenever you have computing resources
available to do so, but heavy-weight submission systems like Torque are overkill.

simple-job-submit lets you specify your computing resources in a yaml config
file, (see config.yaml.example for an example) and once job managers are up and
running, submitting a command to a free node is as simple as:

```
./submit.py any job --command 'commands; to; run;' --config config.yaml
```

sjs will then tell you where it sent the job submission and whether submission
was successful.

Dependencies
------------

- Python 2.7
- pyyaml
- sshpass (if you want to specify ssh passwords in your config for when you absolutely cannot use key pairs, which is... not advisable)

Features
========

sjs supports three types of commands: job, status, and configure. job
submits a job to a job manager, status gives the status (# jobs running,
max # jobs possible to run, job queue) of a job manager, and configure
lets you set job manager parameters (currently only parameter supported
is max # jobs running). Examples:

On manager-1:
```
./job_manager --max-jobs-running 2
```

On manager-2:
```
./job_manager --max-jobs-running 4
```

On manager-3:
```
./job_manager --max-jobs-running 6
```

The submit script has --config command line argument specifying
the yaml config file, and this defaults to 'config.yaml'.

```
./submit manager-1 status
{"status": "OK", "jobs_running": 0, "max_jobs_running": 2, "code": 0, "jobs_queued": "[]"}
```

```
./submit all status
[manager-1] {"status": "OK", "jobs_running": 0, "max_jobs_running": 2, "code": 0, "jobs_queued": "[]"}
[manager-2] {"status": "OK", "jobs_running": 0, "max_jobs_running": 4, "code": 0, "jobs_queued": "[]"}
[manager-3] {"status": "OK", "jobs_running": 0, "max_jobs_running": 6, "code": 0, "jobs_queued": "[]"}
```

```
./submit manager-1 job --command 'echo hello'
{"status": "OK", "message": "job submitted successfully", "code": 0, "job_id": 0}
```

```
./submit manager-1 job --command-file commands.txt
{"status": "OK", "message": "job submitted successfully", "code": 0, "job_id": 1}
{"status": "OK", "message": "job submitted successfully", "code": 0, "job_id": 2}
{"status": "OK", "message": "job submitted successfully", "code": 0, "job_id": 3}
{"status": "OK", "message": "job submitted successfully", "code": 0, "job_id": 4}
{"status": "OK", "message": "job submitted successfully", "code": 0, "job_id": 5}
```

```
./submit manager-1 configure --max-jobs-running 0
{"status": "OK", "old_max_jobs_running": 2, "code": 0, "new_max_jobs_running": 0, "message": "configuration successful"}
```

```
./submit all status
[manager-1] {"status": "OK", "jobs_running": 0, "max_jobs_running": 0, "code": 0, "jobs_queued": "[]"}
[manager-2] {"status": "OK", "jobs_running": 0, "max_jobs_running": 4, "code": 0, "jobs_queued": "[]"}
[manager-3] {"status": "OK", "jobs_running": 0, "max_jobs_running": 6, "code": 0, "jobs_queued": "[]"}
```

How It Works
============

All communication is done over ssh and via named pipes. This means
that you get advantages of ssh + Unix user / file permissions. The
job manager determines when jobs are finished by catching SIGCHLD
in a custom handler.

Licensing
=========

simple-job-submit is licensed under the [MIT license (MIT)](https://opensource.org/licenses/MIT).
