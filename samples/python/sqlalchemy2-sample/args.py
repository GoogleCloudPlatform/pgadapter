""" Copyright 2024 Google LLC

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

import argparse
from pgadapter import in_process_pgadapter_host, in_process_pgadapter_port


def parse_arguments():
  parser = argparse.ArgumentParser(description='Run SQLAlchemy sample.')
  parser.add_argument('-e', '--emulator', required=False, default=None,
                      help="Set this option to 'True' to force PGAdapter to "
                           "connect to the emulator.")
  parser.add_argument('-p', '--project', default="my-project",
                      help="The Google Cloud project containing the Cloud "
                           "Spanner instance that PGAdapter should connect to.")
  parser.add_argument('-i', '--instance', default="my-instance",
                      help="The Cloud Spanner instance that PGAdapter should "
                           "connect to.")
  parser.add_argument('-d', '--database', default="my-database",
                      help="The Cloud Spanner database that SQLAlchemy should "
                           "connect to.")
  parser.add_argument('-c', '--credentials', required=False,
                      help="The credentials file that PGAdapter should use to "
                           "connect to Cloud Spanner. If None, then the sample "
                           "application will try to use the default credentials"
                           " in the environment.")

  # The following arguments is for connecting to a manually started PGAdapter.
  parser.add_argument('--host', type=str, help='host to connect to',
                      required=False, default=in_process_pgadapter_host)
  parser.add_argument('--port', type=int, help='port number to connect to',
                      required=False, default=in_process_pgadapter_port)
  return parser.parse_args()
