#!/usr/bin/env python
import sys
import yaml
import subprocess
import shlex

host = sys.argv[1]
cmd = sys.argv[2]
config_file = sys.argv[3]

with open(config_file) as f:
    config = yaml.safe_load(f)

settings = config['hosts'][host]
subprocess.call(['ssh', '-A', host] + shlex.split("'cd %s;" % settings['root'] + "echo job:\"%s\": > %s" % (cmd, settings['pipe']) + "'"))
