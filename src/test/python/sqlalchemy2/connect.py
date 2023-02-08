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
import argparse

from psycopg import sql
# from psycopg.extensions import register_adapter, AsIs
# from psycopg.extras import Json
from sqlalchemy import create_engine, text, bindparam
import sys


def create_test_engine(autocommit=False, options=""):
  parser = argparse.ArgumentParser(description='Run SQLAlchemy tests.')
  parser.add_argument('host', type=str, help='host to connect to')
  parser.add_argument('port', type=int, help='port number to connect to')
  parser.add_argument('database', type=str, help='database to connect to')
  args = parser.parse_args()

  conn_string = "postgresql+psycopg://user:password@{host}:{port}/d{options}".format(
    host=args.host, port=args.port, options=options)
  if args.host == "":
    if options == "":
      conn_string = conn_string + "?host=/tmp"
    else:
      conn_string = conn_string + "&host=/tmp"
  conn = create_engine(conn_string, future=True)
  if autocommit:
    return conn.execution_options(isolation_level="AUTOCOMMIT")
  return conn


# Adapts identifiers as-is. This is used to insert PostgreSQL style parameters
# into psycopg2-formatted strings.
def adapt_identifier(identifier):
  return AsIs(identifier.string)


def adapt_dict(value):
  return Json(value)


# Generates a `prepare <name> as <stmt>` statement.
def generate_prepare_statement(name, stmt):
  prepare_stmt = text("prepare {} as {}".format(name, stmt))
  params = {}
  for index, param in enumerate(stmt.params):
    params[param] = sql.Identifier("${}".format(index+1))
  return prepare_stmt, params

def generate_execute_statement(name, stmt):
  param_names = [""] * len(stmt.params)
  params = {}
  for index, param in enumerate(stmt.params):
    param_names[index] = ":{}".format(str(param))
    params[param] = bindparam(str(param))
  execute_stmt = text("execute {} ({})".format(name, ",".join(param_names)))
  # execute_stmt.bindparams(params)
  return execute_stmt


# register_adapter(sql.Identifier, adapt_identifier)
# register_adapter(dict, adapt_dict)
