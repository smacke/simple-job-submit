#!/usr/bin/env python
import os
import sys
import yaml
import subprocess
import shlex
import json
import itertools
import argparse
import re

errors = {'eexists': 2}
all_patts = ['*', 'all']

def build_remote_command(cmd_type, manager_settings, command):
    remote_command = "%s %s" % (cmd_type, command)
    if 'password' in manager_settings:
        remote_command = ('sshpass -p %s ' % manager_settings['password']) + remote_command
    return remote_command

def get_host_from_settings(manager_settings):
    host = manager_settings['host']
    if 'user' in manager_settings:
        host = settings['user'] + '@' + host
    return host

def get_port_from_settings(manager_settings):
    if 'port' in manager_settings:
        return int(manager_settings['port'])
    else:
        return None

def build_ssh_command(manager_settings, command, quiet=False):
    host = get_host_from_settings(manager_settings)
    port = get_port_from_settings(manager_settings)
    command = "%s '%s'" % (host, command)
    ssh = "ssh -A"
    if port is not None:
        ssh += (" -p %d" % port)
    if quiet:
        ssh += " -q"
    return build_remote_command(ssh, manager_settings, command)

def build_scp_command(manager_settings, from_file, to_file, recursive=False, quiet=False):
    host = get_host_from_settings(manager_settings)
    port = get_port_from_settings(manager_settings)
    command = "%s %s:%s" % (from_file, host, to_file)
    scp = "scp"
    if recursive:
        scp += " -r"
    if port is not None:
        scp += (" -P %d" % port)
    if quiet:
        scp += " -q"
    return build_remote_command(scp, manager_settings, command)

def build_rsync_command(manager_settings, from_file, to_file):
    host = get_host_from_settings(manager_settings)
    port = get_port_from_settings(manager_settings)
    command = "%s %s:%s" % (from_file.strip('/'), host, to_file)
    rsync = "rsync -rvz"
    if port is not None:
        rsync += (" -e 'ssh -p %d'" % port)
    rsync += " --progress"
    return build_remote_command(rsync, manager_settings, command)

def check_exists_remote(settings, check_path, check_flag="-e"):
    return subprocess.call(build_ssh_command(settings, "[ %s %s ]" % (check_flag, check_path),
        quiet=True), shell=True) == 0

def run_command(cmd_json, args, parser, config, suppress_output=False):
    if args.manager in all_patts:
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

def handle_submit_job_any(cmd_json, args, parser, config):
    best_manager = None
    most_slots_least_queued = (0, float('inf'))
    all_managers_0_max = True
    for manager in config['managers']:
        args.manager = manager
        status = handle_stat(cmd_json, args, parser, config, suppress_output=True)
        if status['code'] > 0:
            sys.stderr.write("[%s] warning: manager had error during stating: %s\n" % (args.manager, status['message']))
            continue
        num_slots = status['max_jobs_running'] - status['jobs_running']
        all_managers_0_max = all_managers_0_max and status['max_jobs_running'] <= 0
        if num_slots < 0:
            sys.sdterr.write("[%s] warning: manager running more jobs than has slots\n" % manager)
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
    return handle_submit_job_nocheck_status(cmd_json, args, parser, config)

def handle_submit_job_nocheck_status(cmd_json, args, parser, config):
    cmd_json['type'] = 'submit_job'
    # this function does not do a status check before submission
    # as with handle_job, it assumes cmd_json['run'] is set
    return run_command(cmd_json, args, parser, config)

def handle_submit_job(cmd_json, args, parser, config):
    cmd_json['type'] = 'submit_job'
    # this function assumes cmd_json['run'] already set

    if args.manager in all_patts:
        for manager in config['managers']:
            args.manager = manager
            print ("[%s]" % manager),
            handle_submit_job(cmd_json, args, parser, config)
    elif args.manager == 'any':
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
    if args.manager == 'any' or args.manager in all_patts:
        # TODO: make job ids unique across all managers, then maybe 'any' makes sense
        parser.error("job cancellation requires specific manager")
    if args.jid_cancel not in all_patts:
        args.jid_cancel = int(args.jid_cancel)
    cmd_json['job_to_cancel'] = args.jid_cancel
    return run_command(cmd_json, args, parser, config)

def tmux_and_start(settings, args):
    return subprocess.call(build_ssh_command(settings,
        ("export PATH=\"$PATH\":/usr/local/bin; cd %s; " + ("make; " if args.make else "") + \
                "tmux new -s %s -d; tmux send -t %s:0 " + \
                "\"./job_manager.py --max-jobs-running %d\" ENTER;") % \
        (settings['project_root'], args.manager, args.manager, settings['default_max_jobs'])),
        shell=True) == 0

def handle_deploy(cmd_json, args, parser, config):
    # TODO: this one is different; maybe should have different method signature
    if args.manager in all_patts:
        for manager in config['managers']:
            args.manager = manager
            handle_deploy(cmd_json, args, parser, config)
    elif args.manager == 'any':
        parser.error('deployment requires specific manager or all')
    else:
        settings = config['managers'][args.manager]
        if check_exists_remote(settings, settings['project_root']):
            sys.stderr.write("[%s] warning: already deployed. will update job manager unless running\n" % args.manager)
        else:
            subprocess.call(build_ssh_command(settings,
                "git clone %s %s" % (config['deployment']['project_url'],
                    settings['project_root'])), shell=True)
        if handle_check_running(cmd_json, args, parser, config, suppress_output=True):
            sys.stderr.write("[%s] error: already deployed, already running\n" % args.manager)
            return
        subprocess.call(build_scp_command(settings,
            './job_manager.py', settings['project_root']), shell=True)
        if tmux_and_start(settings, args):
            print "[%s] startup successful" % args.manager
        else:
            sys.stderr.write("[%s] something went wrong on start!\n" % args.manager)
            return

def handle_check_running(cmd_json, args, parser, config, suppress_output=False):
    if args.manager in all_patts:
        for manager in config['managers']:
            args.manager = manager
            handle_check_running(cmd_json, args, parser, config, suppress_output)
        return
    settings = config['managers'][args.manager]
    check_path = os.path.join(settings['project_root'], settings['pipe'])
    # check for existence of named pipe
    is_running = check_exists_remote(settings, check_path, "-p")
    if not suppress_output:
        if is_running:
            print "[%s] I am running" % args.manager
        else:
            print "[%s] I am NOT running" % args.manager
    return is_running

def handle_force(cmd_json, args, parser, config):
    if args.cmd is None:
        parser.error("command type %s requires cmd" % args.type)
    elif args.manager in all_patts:
        for manager in config['managers']:
            args.manager = manager
            handle_force(cmd_json, args, parser, config)
        return
    elif args.manager == 'any':
        parser.error('force requires specific manager or all')
    else:
        print "[%s] calling command: %s" % (args.manager, args.cmd)
        settings = config['managers'][args.manager]
        if handle_check_running(cmd_json, args, parser, config, suppress_output=True):
            status = handle_stat(cmd_json, args, parser, config, suppress_output=True)
            num_jobs_running = int(status['jobs_running'])
            if num_jobs_running > 0:
                sys.stderr.write("[%s] %d job(s) running, refuse force\n" % \
                        (args.manager, num_jobs_running, num_jobs_queued))
                return
        subprocess.call(build_ssh_command(settings,
                "cd %s; %s" % (settings['project_root'], args.cmd)), shell=True)

def handle_upload_data(cmd_json, args, parser, config):
    dataset = args.dataset
    if args.manager in all_patts:
        for manager in config['managers']:
            args.manager = manager
            args.dataset = dataset # since this gets fiddled with
            handle_upload_data(cmd_json, args, parser, config)
        return
    elif args.manager == 'any':
        parser.error('upload requires specific manager or all')
    elif dataset in all_patts:
        for dataset in config['deployment']['datasets']:
            args.dataset = dataset
            handle_upload_data(cmd_json, args, parser, config)
    else:
        print "[%s] uploading %s data..." % (args.manager, args.dataset)
        settings = config['managers'][args.manager]
        datapath = config['managers'][args.manager]['datadir']
        upload_dataset = config['deployment']['datasets'][args.dataset]
        check_path = os.path.join(datapath, os.path.basename(upload_dataset))
        if check_exists_remote(settings, check_path):
            sys.stderr.write("[%s] warning: path %s already exists, skipping\n" % (args.manager, check_path))
            return
        if subprocess.call(build_rsync_command(settings, upload_dataset, datapath), shell=True) != 0:
            sys.stderr.write("[%s] warning: something went wrong calling rsync to path %s" % (args.manager, datapath))
            return

def handle_start(cmd_json, args, parser, config):
    if args.manager in all_patts:
        for manager in config['managers']:
            args.manager = manager
            handle_start(cmd_json, args, parser, config)
    elif args.manager == 'any':
        parser.error('start requires specific manager or all')
    else:
        if handle_check_running(cmd_json, args, parser, config, suppress_output=True):
            sys.stderr.write("[%s] error: already running\n" % args.manager)
            return
        settings = config['managers'][args.manager]
        project = settings['project_root']
        if not check_exists_remote(settings, settings['project_root']):
            sys.stderr.write("[%s] error: not deployed to project root %s yet\n" % (args.manager, project))
            return
        if tmux_and_start(settings, args):
            print "[%s] startup successful" % args.manager
        else:
            sys.stderr.write("[%s] something went wrong on start!" % args.manager)
            return

def handle_shutdown(cmd_json, args, parser, config):
    if args.manager in all_patts:
        for manager in config['managers']:
            args.manager = manager
            handle_shutdown(cmd_json, args, parser, config)
    elif args.manager == 'any':
        parser.error('shutdown requires specific manager or all')
    else:
        if not handle_check_running(cmd_json, args, parser, config, suppress_output=True):
            sys.stderr.write("[%s] error: not running; cannot shutdown\n" % args.manager)
            return
        status = handle_stat(cmd_json, args, parser, config, suppress_output=True)
        num_jobs_running = int(status['jobs_running'])
        num_jobs_queued = int(status['num_jobs_queued'])
        if num_jobs_running > 0 or num_jobs_queued > 0:
            sys.stderr.write("[%s] %d job(s) running and %d job(s) queued, refuse shutdown\n" % \
                    (args.manager, num_jobs_running, num_jobs_queued))
            return
        else:
            cmd_json['type'] = 'shutdown'
            settings = config['managers'][args.manager]
            print ("[%s]" % args.manager),
            ret = run_command(cmd_json, args, parser, config)
            # TODO: there may be a race here
            subprocess.call(build_ssh_command(settings,
                "export PATH=\"$PATH\":/usr/local/bin; " + \
                        "tmux kill-session -t %s;" % args.manager), shell=True)
            return ret


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
            'force': handle_force,
            'upload-data': handle_upload_data,
            'check-running': handle_check_running,
            'start': handle_start,
            'shutdown': handle_shutdown,
            }
    parser = argparse.ArgumentParser(description="Client for talking to job managers.")
    parser.add_argument('type', help="type of command to run -- either submit (to submit job), stat (stat current jobs), configure (set manager parameters), cancel (cancel jobs), deploy (deploy job managers from config), force (run command immediately), upload-data (upload data to managers), check-running (self-explanatory), start, or shutdown")
    parser.add_argument('manager', help="which job manager to run command on. special are all, any (any tries to find non-saturated manager)")
    parser.add_argument('--config', dest='config', default='config.yaml', help="yaml config file with job manager locations. see example for format")
    parser.add_argument('--command', dest='cmd', default=None, help="if type is submit, the command to run as a job")
    parser.add_argument('--jid', dest='jid_cancel', default=None, help="if type is cancel, which job to cancel ('all' cancels all jobs)")
    parser.add_argument('--command-file', dest='cmd_file', default=None, help="if type is submit, the newline-separated file of commands to run")
    parser.add_argument('--max-jobs-running', dest='max_jobs', type=int, default=None, help="if type is configure, new maximum # of jobs running")
    parser.add_argument('--git', dest='git', default=False, action='store_true', help="whether to do a 'git pull' before executing commands")
    parser.add_argument('--make', dest='make', default=False, action='store_true', help="whether to do a 'make' before executing commands")
    parser.add_argument('--dataset', dest='dataset', default='all', help="which dataset(s) to copy to specified manager")
    args = parser.parse_args()
    main(args)
