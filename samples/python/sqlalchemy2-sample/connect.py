""" Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

from pgadapter import in_process_pgadapter_host, in_process_pgadapter_port
from args import parse_arguments
from sqlalchemy import create_engine


# Creates a database engine that can be used with the sample.py script in this
# directory. This function checks whether an in-process PGAdapter instance has
# been started, and if so, uses that instance. Otherwise, it connects to the
# host and port number given in the command line arguments.
def create_test_engine(autocommit: bool = False, options: str = ""):
  args = parse_arguments()
  # Check if PGAdapter has been started in-process. If so, we'll connect to that
  # instance.
  if in_process_pgadapter_host is not None and in_process_pgadapter_port is not None:
    args.host = in_process_pgadapter_host
    args.port = in_process_pgadapter_port

  conn_string = "postgresql+psycopg://user:password@{host}:{port}/" \
                "{database}{options}".format(host=args.host,
                                             port=args.port,
                                             database=args.database,
                                             options=options)
  if args.host == "":
    if options == "":
      conn_string = conn_string + "?host=/tmp"
    else:
      conn_string = conn_string + "&host=/tmp"
  engine = create_engine(conn_string, future=True)
  if autocommit:
    engine = engine.execution_options(isolation_level="AUTOCOMMIT")
  return engine
