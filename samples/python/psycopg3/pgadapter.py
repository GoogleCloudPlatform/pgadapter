# Copyright 2023 Google LLC All rights reserved.
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

"""Utility for starting and stopping PGAdapter in an embedded container

Defines functions for starting and stopping PGAdapter in an embedded Docker
container. Requires that Docker is installed on the local system.
"""

import io
import json
import os
import socket
import time
import google.auth
import google.oauth2.credentials
import google.oauth2.service_account
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


def start_pgadapter(project: str,
                    instance: str,
                    credentials: str = None) -> (DockerContainer, str):
  """Starts PGAdapter in an embedded Docker container

  Starts PGAdapter in an embedded Docker container and returns the TCP port
  number where PGAdapter is listening for incoming connections. You can Use any
  standard PostgreSQL driver to connect to this port.

  Parameters
  ----------
  project : str
      The Google Cloud project that PGAdapter should connect to.
  instance : str
      The Cloud Spanner instance that PGAdapter should connect to.
  credentials : str or None
      The credentials file that PGAdapter should use. If None, then this
      function will try to load the default credentials from the environment.

  Returns
  -------
  container, port : tuple[DockerContainer, str]
      The Docker container running PGAdapter and
      the port where PGAdapter is listening. Connect to this port on localhost
      with a standard PostgreSQL driver to connect to Cloud Spanner.
  """

  # Start PGAdapter in a Docker container
  container = DockerContainer("gcr.io/cloud-spanner-pg-adapter/pgadapter") \
    .with_exposed_ports(5432) \
    .with_command("   -p " + project
                  + " -i " + instance
                  + " -x -c /credentials.json")
  container.start()
  # Determine the credentials that should be used by PGAdapter and write these
  # to a file in the container.
  credentials_info = _determine_credentials(credentials)
  container.exec("sh -c 'cat <<EOT >> /credentials.json\n"
                 + json.dumps(credentials_info, indent=0)
                 + "\nEOT'")
  # Wait until PGAdapter has started and is listening on the exposed port.
  wait_for_logs(container, "PostgreSQL version:")
  port = container.get_exposed_port("5432")
  _wait_for_port(port=int(port))
  return container, port


def _determine_credentials(credentials: str):
  if credentials is None:
    explicit_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
  else:
    explicit_file = credentials
  if explicit_file is None:
    credentials, _ = google.auth.default()
    if type(credentials).__name__ == \
       google.oauth2.credentials.Credentials.__name__:
      info = json.loads(credentials.to_json())
      info["type"] = "authorized_user"
    else:
      raise ValueError("GOOGLE_APPLICATION_CREDENTIALS has not been set "
                       "and no explicit credentials were supplied")
  else:
    with io.open(explicit_file, "r") as file_obj:
      info = json.load(file_obj)
  return info


def _wait_for_port(port: int, poll_interval: float = 0.1, timeout: float = 5.0):
  start = time.time()
  while True:
    try:
      with socket.create_connection(("localhost", port), timeout=timeout):
        break
    except OSError:
      duration = time.time() - start
      if timeout and duration > timeout:
        raise TimeoutError("container did not listen on port {} in {} seconds"
                           .format(port, timeout))
      time.sleep(poll_interval)
