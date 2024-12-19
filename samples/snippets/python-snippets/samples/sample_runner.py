# Copyright 2024 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

global host, port, database

def parse_arguments():
    global host, port, database
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python <sample-name>.py <database-name> [host:port]")
        exit(1)
    
    database = sys.argv[1]
    host = "localhost"
    port = 5432
    if len(sys.argv) == 3:
        host_and_port = sys.argv[2]
        if not ":" in host_and_port:
            print("Invalid host:port argument: " + host_and_port)
            exit(1)
        host = host_and_port.split(":", 1)[0]
        port = int(host_and_port.split(":", 1)[1])
