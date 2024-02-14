""" Copyright 2022 Google LLC

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

from pgadapter import start_pgadapter
from args import parse_arguments

# Parse command line arguments and potentially start PGAdapter in a Docker
# container.
args = parse_arguments()
container = None

# Check if we should start PGAdapter in-process.
if args.host is None and args.port is None:
  print("Starting PGAdapter in-process")
  # Default to using the emulator if no database has been specified.
  if (args.project == "my-project"
      and args.instance == "my-instance"
      and args.database == "my-database"
      and args.emulator is None):
    use_emulator = True
  else:
    if args.emulator is None:
      use_emulator = False
    else:
      use_emulator = args.emulator == "True"

  # Start PGAdapter in an embedded container.
  container, port = start_pgadapter(args.project,
                                    args.instance,
                                    use_emulator,
                                    args.credentials)
  print("PGAdapter started on port ", port, "\n")
else:
  print("Connecting to PGAdapter on", args.host, ", port", args.port, "\n")

from sample import run_sample

try:
  run_sample()
finally:
  if container is not None:
    container.stop()
