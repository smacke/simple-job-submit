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


def submit(cmd_json, args, parser, config, suppress_output=False):
    if args.manager == 'all':
        for manager in config['managers']:
            args.manager = manager
            print ('[%s]' % args.manager),
            submit(cmd_json, args, parser, config)
        return
    elif args.manager == 'any':
        saturated_but_not_erroring = None
        for manager in config['managers']:
            args.manager = manager
            status = handle_list(cmd_json, args, parser, config, suppress_output=True)
            if status['code'] > 0:
                sys.stderr.write("Warning: manager %s had error during listing: %s" % (args.manager, status['message']))
                continue
            if status['jobs_running'] < status['max_jobs_running']:
                print ('[%s]' % args.manager),
                return handle_job_simple(cmd_json, args, parser, config)
            elif status['max_jobs_running'] > 0:
                saturated_but_not_erroring = manager
        if saturated_but_not_erroring == None:
            raise Exception("all managers have errors or can accept 0 jobs, can't run job!")
        args.manager = saturated_but_not_erroring
        print ('[%s]' % args.manager),
        return handle_job(cmd_json, args, parser, config)

    settings = config['managers'][args.manager]
    host = settings['host']

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
        script = template % (settings['root'], port_fifo, errors['eexists'], cmd_json_str, settings['pipe'], port_fifo, port_fifo)
        script = script.strip()
        port = 22
        if port in settings:
            port = int(settings['port'])
        tocall = "ssh -A -p %d %s '%s'" % (port, host, script)
        if 'password' in settings:
            tocall = ('sshpass -p %s ' % settings['password']) + tocall
        proc = subprocess.Popen(tocall, shell=True, stdout=subprocess.PIPE)
        procout, procerr = proc.communicate()
        if not suppress_output: print procout
        ret = proc.returncode
        if ret == 0:
            return json.loads(procout)
        elif ret == errors['eexists']:
            # then try a new port of this one was already in use
            continue
        else:
            raise Exception("Trying to submit command, got error code %d" % ret)

def handle_job_simple(cmd_json, args, parser, config, suppress_output=False):
    cmd_json['type'] = 'job'
    # this function assumes cmd_json['run'] already set
    return submit(cmd_json, args, parser, config, suppress_output)

def handle_job(cmd_json, args, parser, config, suppress_output=False):
    cmd_json['type'] = 'job'
    if args.cmd is None and args.cmd_file is None:
        parser.error("command type %s requires either cmd or file" % args.type)
    manager = args.manager
    if args.cmd is not None:
        cmd_json['run'] = args.cmd
        submit(cmd_json, args, parser, config, suppress_output)
    if args.cmd_file is not None:
        with open(args.cmd_file, 'r') as f:
            for line in f:
                args.manager = manager # since this gets fiddled with
                # TODO: maybe pass deep copies further down
                cmd_json['run'] = line
                submit(cmd_json, args, parser, config, suppress_output)

def handle_list(cmd_json, args, parser, config, suppress_output=False):
    cmd_json['type'] = 'list'
    return submit(cmd_json, args, parser, config, suppress_output)

def handle_configure(cmd_json, args, parser, config, suppress_output=False):
    cmd_json['type'] = 'configure'
    if args.manager == 'any':
        parser.error("this doesn't make sense; configuration should be specific")
    if args.max_jobs is None:
        parser.error("Configure command needs to specify new max jobs")
    cmd_json['max_jobs'] = args.max_jobs
    return submit(cmd_json, args, parser, config, suppress_output)

def main(args):
    with open(args.config) as f:
        config = yaml.safe_load(f)

    if args.type not in command_type_handle:
        parser.error("Command type must be one of %s" % command_type_handle.keys())

    cmd_json = {'type': args.type, 'git': args.git, 'make': args.make}
    command_type_handle[args.type](cmd_json, args, parser, config)

if __name__=="__main__":
    command_type_handle = {'job': handle_job, 'list': handle_list, 'configure': handle_configure}
    parser = argparse.ArgumentParser(description="Submit job to job manager.")
    parser.add_argument('manager')
    parser.add_argument('type')
    parser.add_argument('--config', dest='config', default='config.yaml')
    parser.add_argument('--command', dest='cmd', default=None)
    parser.add_argument('--command-file', dest='cmd_file', default=None)
    parser.add_argument('--max-jobs-running', dest='max_jobs', type=int, default=None)
    parser.add_argument('--git', dest='git', default=False, action='store_true')
    parser.add_argument('--make', dest='make', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
