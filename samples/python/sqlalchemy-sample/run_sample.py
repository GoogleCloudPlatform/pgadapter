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

import sys


def print_usage():
  print()
  print("Usage: `python run_sample.py <host> <port> <database>`")
  print()


if len(sys.argv) != 4:
  print_usage()
  raise ValueError("Invalid number of arguments. Expected 3")

if not sys.argv[1]:
  print_usage()
  raise ValueError("Invalid host: {}".format(sys.argv[1]))

if not sys.argv[2]:
  print_usage()
  raise ValueError("Invalid port: {}".format(sys.argv[2]))

if not sys.argv[3]:
  print_usage()
  raise ValueError("Invalid database: {}".format(sys.argv[3]))


from sample import run_sample

run_sample()


