#!/usr/bin/env python

import os, sys

readstr = sys.argv[1]

pipeout = os.open('jobs.pipe', os.O_WRONLY)
os.write(pipeout, readstr)
os.close(pipeout)
