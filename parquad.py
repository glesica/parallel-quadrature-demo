#!/usr/bin/python
# File: parquad.py
# Author: George Lesica
# Desc: This program computes an integral numerically in a distributed fashion.

import math, sys, time, getopt
import pp

def usage():
    return """Usage: python parquad.py [--help] [--cpus <n>] --expression <s> 
    --lower <n> --upper <n> --panels <n> [addresses...]
    help         - prints this help and exits
    expression   - an expression in x to be evaluated as the integrand, uses 
                   Python syntax
    lower, upper - range over which to integrate
    panels       - the number of panels to use for the approximation
    cpus         - the number of workers to run in parallel, if omitted it will 
                   be set to the number of processors in the system
    addresses    - addresses of worker nodes in a cluster, usually IP addresses
\n"""

# Read in command line arguments and process them into variables
largs = [
    'help',
    'cpus=',
    'expression=',
    'lower=',
    'upper=',
    'panels=',
]

try:
    opts, args = getopt.gnu_getopt(sys.argv[1:], '', largs)
except getopt.GetoptError, err:
    # print help information and exit:
    sys.stderr.write(str(err) + '\n' + usage())
    sys.exit(1)

cpus = None
expression = None
bounds = {}
panels = None
servers = tuple(args)
multiplier = 1 # To become a command line argument

for o, a in opts:
    if o == '--help':
        sys.stdout.write(usage())
        sys.exit(0)
    elif o == '--cpus':
        try:
            cpus = int(a)
        except ValueError:
            sys.stderr.write('Invalid argument: %s\n' % a + usage())
            sys.exit(1)
    elif o == '--expression':
        x = 0
        try:
            t = eval(a)
        except SyntaxError:
            sys.stderr.write('Invalid argument: %s\n' % a + usage())
            sys.exit(1)
        except ZeroDivisionError:
            pass
        else:
            expression = a
    elif o in ('--lower', '--upper'):
        try:
            bounds[o] = float(a)
        except ValueError:
            sys.stderr.write('Invalid argument: %s\n' % a + usage())
            sys.exit(1)
    elif o == '--panels':
        try:
            panels = int(a)
        except ValueError:
            sys.stderr.write('Invalid argument: %s\n' % a + usage())
            sys.exit(1)

if not expression:
    sys.stderr.write('Required argument missing: --expression\n' + usage())
    sys.exit(1)
elif len(bounds) != 2:
    sys.stderr.write('Required argument missing: --lower or --upper\n' + usage())
    sys.exit(1)
elif not panels:
    sys.stderr.write('Required argument missing: --panels\n' + usage())
    sys.exit(1)

# Set up the function we'll use
def trap(e, a, b, n):
    # Function to integrate over
    def f(x):
        """A generic function in x"""
        return eval(e)
    # Do the quadrature stuff (trapezoid rule)
    step = (b - a) / n
    total = 0.0
    for i in xrange(n):
        total += (f(a + i*step) + f(a + (i+1)*step)) / 2 * step
    return total

# Create the parallel processing handler
if cpus:
    job_server = pp.Server(cpus, ppservers=servers)
else:
    job_server = pp.Server(ppservers=servers)

# Figure out total CPUs
total_cpus = 0
for n, c in job_server.get_active_nodes().items():
    total_cpus += c

print job_server.get_active_nodes()

# Start jobs based on the number of cpus available
parts = total_cpus * multiplier
# Adjust panels to make things even
panels = (panels / parts) * parts
h = (bounds['--upper'] - bounds['--lower']) / parts
# Prepare input tuples
inputs = [(expression, bounds['--lower'] + i*h, bounds['--lower'] + (i+1)*h, panels / parts) for i in xrange(parts)]
# Submit jobs in parallel
jobs = [job_server.submit(trap , i, (), ()) for i in inputs]
# Recover results and sum
result = 0.0
for job in jobs:
    result += job()

sys.stdout.write('%f\n' % result)
