#!/usr/bin/env python
import sys
import yaml
import subprocess
import shlex
import json
import itertools
import argparse
import re

errors = {'eexists': 2}

def build_remote_command(cmd_type, port_flag, manager_settings, command):
    port = 22
    if port in manager_settings:
        port = int(manager_settings['port'])
    remote_command = "%s %s %d %s" % (cmd_type, port_flag, port, command)
    if 'password' in manager_settings:
        remote_command = ('sshpass -p %s ' % manager_settings['password']) + remote_command
    return remote_command

def get_host_from_settings(manager_settings):
    host = manager_settings['host']
    if 'user' in manager_settings:
        host = settings['user'] + '@' + host
    return host

def build_ssh_command(manager_settings, command):
    host = get_host_from_settings(manager_settings)
    command = "%s '%s'" % (host, command)
    return build_remote_command("ssh -A", "-p", manager_settings, command)

def build_scp_command(manager_settings, from_file, to_file):
    host = get_host_from_settings(manager_settings)
    command = "%s %s:%s" % (from_file, host, to_file)
    return build_remote_command("scp", "-P", manager_settings, command)

def run_command(cmd_json, args, parser, config, suppress_output=False):
    if args.manager == 'all':
        for manager in config['managers']:
            args.manager = manager
            print ('[%s]' % args.manager),
            run_command(cmd_json, args, parser, config)
        return
    elif args.manager == 'any':
        raise Exception("'any' should be reserved for job-submission-handling logic; this exception should be unreachable")

    settings = config['managers'][args.manager]
    template = \
"""
cd %s;
if ! mkfifo %s; then
    exit %d
fi
echo %s > %s;
cat %s; # print the return message; this will be piped back to python
rm %s # clear the port for later use
"""

    for port in itertools.count(1):
        port_fifo = "%d.port" % port
        cmd_json['port'] = port_fifo
        # escaping arbitrary cmd line arguments in bash
        # ref: http://qntm.org/bash
        cmd_json_str = re.escape(json.dumps(cmd_json))
        script = template % (settings['project_root'], port_fifo, errors['eexists'], cmd_json_str, settings['pipe'], port_fifo, port_fifo)
        script = script.strip()
        ssh_command = build_ssh_command(settings, script)
        proc = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.PIPE)
        procout, procerr = proc.communicate()
        if not suppress_output: print procout
        ret = proc.returncode
        if ret == 0:
            return json.loads(procout)
        elif ret == errors['eexists']:
            # then try a new port of this one was already in use
            continue
        else:
            raise Exception("Trying to run command, got error code %d" % ret)

def handle_job_submit_any(cmd_json, args, parser, config):
    best_manager = None
    most_slots_least_queued = (0, float('inf'))
    all_managers_0_max = True
    for manager in config['managers']:
        args.manager = manager
        status = handle_stat(cmd_json, args, parser, config, suppress_output=True)
        if status['code'] > 0:
            sys.stderr.write("[%s] warning: manager had error during stating: %s" % (args.manager, status['message']))
            continue
        num_slots = status['max_jobs_running'] - status['jobs_running']
        all_managers_0_max = all_managers_0_max and status['max_jobs_running'] <= 0
        if num_slots < 0:
            sys.sdterr.write("[%s] warning: manager running more jobs than has slots" % manager)
            continue
        num_queued = status['num_jobs_queued']
        num_slots_num_queued = (-num_slots, num_queued)
        if num_slots_num_queued < most_slots_least_queued:
            # try to run on a manager with a free slot.
            # if that fails, try to run on manager with shortest queue
            most_slots_least_queued = num_slots_num_queued
            best_manager = manager

    if best_manager is None:
        raise Exception("all managers have errors, can't submit job!")
    if all_managers_0_max:
        raise Exception("all managers accepting at most 0 jobs, can't submit job!")

    args.manager = best_manager
    print ('[%s]' % args.manager),
    return handle_job_submit_nocheck_status(cmd_json, args, parser, config)

def handle_submit_job_nocheck_status(cmd_json, args, parser, config):
    cmd_json['type'] = 'submit_job'
    # this function does not do a status check before submission
    # as with handle_job, it assumes cmd_json['run'] is set
    return run_command(cmd_json, args, parser, config)

def handle_submit_job(cmd_json, args, parser, config):
    cmd_json['type'] = 'submit_job'
    # this function assumes cmd_json['run'] already set

    if args.manager == 'any':
        return handle_submit_job_any(cmd_json, args, parser, config)
    else:
        status = handle_stat(cmd_json, args, parser, config, suppress_output=True)
        if status['max_jobs_running'] <= 0:
            print '[%s] warning: manager accepting at most 0 jobs, job will be queued' % args.manager
        return handle_submit_job_nocheck_status(cmd_json, args, parser, config)

def handle_submit_job_entrypoint(cmd_json, args, parser, config):
    cmd_json['type'] = 'submit_job'
    if args.cmd is None and args.cmd_file is None:
        parser.error("command type %s requires either cmd or file" % args.type)
    manager = args.manager
    if args.cmd is not None:
        cmd_json['run'] = args.cmd
        handle_submit_job(cmd_json, args, parser, config)
    if args.cmd_file is not None:
        with open(args.cmd_file, 'r') as f:
            for line in f:
                args.manager = manager # since this gets fiddled with
                # TODO: maybe pass deep copies further down
                cmd_json['run'] = line
                handle_submit_job(cmd_json, args, parser, config)

def handle_stat(cmd_json, args, parser, config, suppress_output=False):
    cmd_json['type'] = 'stat'
    if args.manager == 'any':
        parser.error("this doesn't make sense; stating should be specific")
    return run_command(cmd_json, args, parser, config, suppress_output)

def handle_configure(cmd_json, args, parser, config):
    cmd_json['type'] = 'configure'
    if args.manager == 'any':
        parser.error("this doesn't make sense; configuration should be specific")
    if args.max_jobs is None:
        parser.error("Configure command needs to specify new max jobs")
    cmd_json['max_jobs'] = args.max_jobs
    return run_command(cmd_json, args, parser, config)

def handle_cancel(cmd_json, args, parser, config):
    cmd_json['type'] = 'cancel'
    if args.jid_cancel is None:
        parser.error("need to specify job id to cancel")
    if args.manager == 'any' or args.manager == 'all':
        # TODO: make job ids unique across all managers, then maybe 'any' makes sense
        parser.error("job cancellation requires specific manager")
    cmd_json['job_to_cancel'] = args.jid_cancel
    return run_command(cmd_json, args, parser, config)

def handle_deploy(cmd_json, args, parser, config):
    # TODO: this one is different; maybe should have different method signature
    if args.manager == 'all':
        for manager in config['managers']:
            args.manager = manager
            handle_deploy(cmd_json, args, parser, config)
    else:
        settings = config['managers'][args.manager]
        subprocess.call(build_ssh_command(settings,
                "git clone %s %s" % (config['deployment']['project_url'],
                    settings['project_root'])), shell=True)
        subprocess.call(build_scp_command(settings,
            './job_manager.py', settings['project_root']), shell=True)
        subprocess.call(build_ssh_command(settings,
            "cd %s; export PATH=\"$PATH\":/usr/local/bin; tmux new -s %s -d; tmux send -t %s:0 \"./job_manager.py --max-jobs-running %d\" ENTER;" % \
            (settings['project_root'], args.manager, args.manager, settings['default_max_jobs'])), shell=True)

def main(args):
    with open(args.config) as f:
        config = yaml.safe_load(f)

    if args.type not in command_type_handle:
        parser.error("Command type must be one of %s" % command_type_handle.keys())

    cmd_json = {'type': args.type, 'git': args.git, 'make': args.make}
    command_type_handle[args.type](cmd_json, args, parser, config)

if __name__=="__main__":
    command_type_handle = {
            'submit': handle_submit_job_entrypoint, 
            'stat': handle_stat, 
            'configure': handle_configure,
            'cancel': handle_cancel,
            'deploy': handle_deploy,
            }
    parser = argparse.ArgumentParser(description="Client for talking to job managers.")
    parser.add_argument('type', help="type of command to run -- either submit (to submit job), stat (stat current jobs), configure (set manager parameters), cancel (cancel jobs), or deploy (deploy job managers from config)")
    parser.add_argument('manager', help="which job manager to run command on. special are all, any (any tries to find non-saturated manager)")
    parser.add_argument('--config', dest='config', default='config.yaml', help="yaml config file with job manager locations. see example for format")
    parser.add_argument('--command', dest='cmd', default=None, help="if type is submit, the command to run as a job")
    parser.add_argument('--jid', dest='jid_cancel', type=int, default=None, help="if type is cancel, which job to cancel")
    parser.add_argument('--command-file', dest='cmd_file', default=None, help="if type is submit, the newline-separated file of commands to run")
    parser.add_argument('--max-jobs-running', dest='max_jobs', type=int, default=None, help="if type is configure, new maximum # of jobs running")
    parser.add_argument('--git', dest='git', default=False, action='store_true', help="whether to do a 'git pull' before executing commands")
    parser.add_argument('--make', dest='make', default=False, action='store_true', help="whether to do a 'make' before executing commands")
    args = parser.parse_args()
    main(args)
