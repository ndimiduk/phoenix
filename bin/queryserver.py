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

import os
import subprocess
import sys
import phoenix_utils

phoenix_utils.setPath()

if os.name == 'nt':
    args = subprocess.list2cmdline(sys.argv[1:])
else:
    import pipes    # pipes module isn't available on Windows
    args = " ".join([pipes.quote(v) for v in sys.argv[1:]])

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = os.getenv('HBASE_CONF_DIR', phoenix_utils.hbase_conf_path)

#    " -Xdebug -Xrunjdwp:transport=dt_socket,address=5005,server=y,suspend=n " + \
#    " -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true" + \
java_cmd = 'java -cp "' + hbase_config_path + os.pathsep + phoenix_utils.phoenix_queryserver_jar + '"' + \
    " -Dlog4j.configuration=file:" + os.path.join(phoenix_utils.current_dir, "log4j.properties") + \
    " -Dpsql.root.logger=INFO,console" + \
    " org.apache.phoenix.queryserver.server.Main " + args

exitcode = subprocess.call(java_cmd, shell=True)
sys.exit(exitcode)
