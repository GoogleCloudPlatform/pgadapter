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
import psycopg2.extras as extras
import sys


def create_connection(host, port):
  try:
    connection = pg.connect(database="my-database",
                            host=host,
                            port=port)
    connection.autocommit = True
    return connection
  except Exception as e:
    print(e)
    return None


def execute_many(sql, parameters, host, port):
  connection = create_connection(host, port)
  if connection == None:
    return
  try:
    cursor = connection.cursor()
    cursor.executemany(sql, parameters)
    print(cursor.rowcount)

  except Exception as e:
    print(e)
  finally:
    cursor.close()
    connection.close()


def execute_batch(sql, parameters, host, port, page_size = None):
  connection = create_connection(host, port)
  if connection == None:
    return
  try:
    cursor = connection.cursor()
    if page_size is None:
      extras.execute_batch(cursor, sql, parameters)
    else:
      extras.execute_batch(cursor, sql, parameters, page_size)

    print(cursor.rowcount)
  except Exception as e:
    print(e)
  finally:
    cursor.close()
    connection.close()

def execute_values(sql, parameters, host, port, page_size = None):
  connection = create_connection(host, port)
  if connection == None:
    return
  try:
    cursor = connection.cursor()
    if page_size is None:
      extras.execute_values(cursor, sql, parameters)
    else:
      extras.execute_values(cursor, sql, parameters, page_size)

    print(cursor.rowcount)
  except Exception as e:
    print(e)
  finally:
    cursor.close()
    connection.close()


def parse_normal_parameters(parameters):
  parameters_list = []
  num_par = int(parameters[0])
  i = 1
  while i < len(parameters):
    temp = []
    j = i
    while j < i+num_par:
      temp.append(parameters[j])
      j += 1
    parameters_list.append(tuple(temp))

    i = j
  return parameters_list


def parse_named_parameters(parameters):
  parameters_list = []
  i = 1
  num_par = int(parameters[0])
  while i < len(parameters):
    temp = {}
    j = i
    while j < i+num_par*2:
      temp[parameters[j]] = parameters[j+1]
      j += 2
    parameters_list.append(temp)

    i = j
  return parameters_list


def parse(statement_type, parameters):
  if 'named' in statement_type:
    return statement_type[6:], parse_named_parameters(parameters)
  else:
    return statement_type, parse_normal_parameters(parameters)



if __name__ == '__main__':
  host = sys.argv[1]
  port = sys.argv[2]
  statement_type = sys.argv[3]
  sql = sys.argv[4]
  try:
    parameters = tuple(sys.argv[5:])
    statement_type, parameters = parse(statement_type, parameters)
  except Exception as e:
    print(e)
    sys.exit(0)
  if statement_type == 'execute_many':
    execute_many(sql, parameters, host, port)
  elif statement_type == 'execute_batch':
    execute_batch(sql, parameters, host, port)
  elif statement_type == 'execute_values':
    execute_values(sql, parameters, host, port)
  else:
    print('Invalid Statement Type', statement_type)
