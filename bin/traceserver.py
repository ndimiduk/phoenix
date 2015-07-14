#!/usr/bin/env python
############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
############################################################################

#
# Script to handle launching the trace server process.
#
# usage: traceserver.py [start]
#

#TODO - Code is in progress. 
import datetime
import getpass
import os
import os.path
import signal
import subprocess
import sys
import tempfile

try:
    import daemon
    daemon_supported = True
except ImportError:
    # daemon script not supported on some platforms (windows?)
    daemon_supported = False

import phoenix_utils

phoenix_utils.setPath()

command = None
args = sys.argv

if len(args) > 1:
    if args[1] == 'start':
        command = 'start'


# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = phoenix_utils.hbase_conf_dir

# default paths ## TODO: add windows support
java_home = os.getenv('JAVA_HOME')
hbase_pid_dir = os.path.join(tempfile.gettempdir(), 'phoenix')
phoenix_log_dir = os.path.join(tempfile.gettempdir(), 'phoenix')
phoenix_file_basename = 'phoenix-%s-tracing-webapp' % getpass.getuser()
phoenix_log_file = '%s.log' % phoenix_file_basename
phoenix_out_file = '%s.out' % phoenix_file_basename
phoenix_pid_file = '%s.pid' % phoenix_file_basename

# load hbase-env.??? to extract JAVA_HOME, HBASE_PID_DIR, HBASE_LOG_DIR
hbase_env_path = None
hbase_env_cmd  = None
if os.name == 'posix':
    hbase_env_path = os.path.join(hbase_config_path, 'hbase-env.sh')
    hbase_env_cmd = ['bash', '-c', 'source %s && env' % hbase_env_path]
elif os.name == 'nt':
    hbase_env_path = os.path.join(hbase_config_path, 'hbase-env.cmd')
    hbase_env_cmd = ['cmd.exe', '/c', 'call %s & set' % hbase_env_path]
if not hbase_env_path or not hbase_env_cmd:
    print >> sys.stderr, "hbase-env file unknown on platform %s" % os.name
    sys.exit(-1)

hbase_env = {}
if os.path.isfile(hbase_env_path):
    p = subprocess.Popen(hbase_env_cmd, stdout = subprocess.PIPE)
    for x in p.stdout:
        (k, _, v) = x.partition('=')
        hbase_env[k.strip()] = v.strip()

if hbase_env.has_key('JAVA_HOME'):
    java_home = hbase_env['JAVA_HOME']
if hbase_env.has_key('HBASE_PID_DIR'):
    hbase_pid_dir = hbase_env['HBASE_PID_DIR']
if hbase_env.has_key('HBASE_LOG_DIR'):
    phoenix_log_dir = hbase_env['HBASE_LOG_DIR']
if hbase_env.has_key('PHOENIX_TRACESERVER_OPTS'):
    opts = hbase_env['PHOENIX_TRACESERVER_OPTS']

log_file_path = os.path.join(phoenix_log_dir, phoenix_log_file)
out_file_path = os.path.join(phoenix_log_dir, phoenix_out_file)
pid_file_path = os.path.join(hbase_pid_dir, phoenix_pid_file)

if java_home:
    java = os.path.join(java_home, 'bin', 'java')
else:
    java = 'java'

#    " -Xdebug -Xrunjdwp:transport=dt_socket,address=5005,server=y,suspend=n " + \
#    " -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true" + \
java_cmd = '%(java)s -jar ' + hbase_config_path + os.pathsep + phoenix_utils.phoenix_traceserver_jar

if command == 'start':
    if not daemon_supported:
        print >> sys.stderr, "daemon mode not supported on this platform"
        sys.exit(-1)

    # run in the background
    d = os.path.dirname(out_file_path)
    if not os.path.exists(d):
        os.makedirs(d)
    with open(out_file_path, 'a+') as out:
        context = daemon.DaemonContext(
            pidfile = daemon.PidFile(pid_file_path, 'Trace Server already running, PID file found: %s' % pid_file_path),
            stdout = out,
            stderr = out,
        )
        print 'starting Trace Server, logging to %s' % log_file_path
        with context:
            # this block is the main() for the forked daemon process
            child = None
            cmd = java_cmd % {'java': java, 'root_logger': 'INFO,DRFA', 'log_dir': phoenix_log_dir, 'log_file': phoenix_log_file}

            # notify the child when we're killed
            def handler(signum, frame):
                if child:
                    child.send_signal(signum)
                sys.exit(0)
            signal.signal(signal.SIGTERM, handler)

            print '%s launching %s' % (datetime.datetime.now(), cmd)
            child = subprocess.Popen(cmd.split())
            sys.exit(child.wait())
