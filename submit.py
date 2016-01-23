#!/usr/bin/env python
import sys
import yaml
import subprocess
import shlex
import json
import itertools
import re

errors = {'eexists': 2}

def escape_quotes(s):
    return s.replace('"', '\\"')

template ="""
cd %s;
if ! mkfifo %s; then
    exit %d
fi
echo %s > %s;
rm %s
"""

host = sys.argv[1]
cmd = sys.argv[2]
config_file = sys.argv[3]

with open(config_file) as f:
    config = yaml.safe_load(f)

settings = config['hosts'][host]

job = {'type': 'job',
       'job' :  cmd}

for port in itertools.count(1):
    port_fifo = "%d.port" % port
    job['port'] = port_fifo
    # escaping arbitrary cmd line arguments in bash
    # ref: http://qntm.org/bash
    job_str = re.escape(json.dumps(job))
    script = template % (settings['root'], port_fifo, errors['eexists'], job_str, settings['pipe'], port_fifo)
    script = script.strip()
    if not subprocess.call("ssh -A %s '%s' >> submit.log 2>>submit.err" % (host, script), shell=True) == errors['eexists']:
        break
