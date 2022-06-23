''' Copyright 2022 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
'''

import psycopg2 as pg
import sys
import pytz
import datetime


def create_connection(port):
  try:
    connection = pg.connect(user="postgres",
                            database="postgres",
                            host="localhost",
                            port=port)
    connection.autocommit = True
    return connection
  except Exception as e:
    print(e)
    return None


def execute_query(sql, parameters, port):
  connection = create_connection(port)
  if connection == None:
    return
  try:
    cursor = connection.cursor()
    cursor.execute(sql, parameters)
    for row in cursor:
      print(row)
  except Exception as e:
    print(e)
  finally:
    cursor.close()
    connection.close()


def execute_update(sql, parameters, port):
  connection = create_connection(port)
  if connection == None:
    return
  try:
    cursor = connection.cursor()
    cursor.execute(sql, parameters)
    print(cursor.rowcount)
  except Exception as e:
    print(e)
  finally:
    cursor.close()
    connection.close()


def parse_named_parameters(parameters):
  parameters_dic = {}
  i = 0
  while i < len(parameters):
    parameters_dic[parameters[i]] = parameters[i + 1]
    i += 2
  return parameters_dic


def parse_named_data_type_parameters(parameters):
  parameters_dic = {}
  i = 0
  while i < len(parameters):
    parameters_dic[parameters[i]] = eval(parameters[i+1]+'('+ parameters[i + 2]+')')
    i += 3
  return parameters_dic


def parse(statement_type, parameters):
  if 'data_type' in statement_type:
    return statement_type[10:], parse_named_data_type_parameters(parameters)
  else:
    return statement_type, parse_named_parameters(parameters)


if __name__ == '__main__':
  port = sys.argv[1]
  statement_type = sys.argv[2]
  sql = sys.argv[3]
  try:
    parameters = tuple(sys.argv[4:])
    statement_type, parameters = parse(statement_type, parameters)
  except Exception as e:
    print(e)
    sys.exit(0)
  if statement_type == 'query':
    execute_query(sql, parameters, port)
  elif statement_type == 'update':
    execute_update(sql, parameters, port)
  else:
    print('Invalid Statement Type', statement_type)
