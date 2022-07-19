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

def create_connection(port):
  try:
    connection = pg.connect(user="postgres",
                            database="postgres",
                            host="localhost",
                            port=port)
    return connection
  except Exception as e:
    print(e)
    return None

def set_property(connection, property, value):
  if property == 'autocommit':
    connection.autocommit = bool(value)
  elif property == 'deferrable':
    connection.deferrable = bool(value)
  elif property == 'isolation_level':
    if value == '0':
      connection.set_isolation_level(0)
    else:
      connection.isolation_level = int(value)
  elif property == 'readonly':
    connection.readonly = bool(value)
  else:
    raise Exception('Trying to set Invalid Property '+property)

def set_session_property(connection, sql):
  properties = sql.split()
  i = 0
  arguements_string = '('
  while i < len(properties):
    arguements_string += properties[i] + ' = ' + properties[i+1]
    i += 2
    if i < len(properties):
      arguements_string += ' , '
  arguements_string += ')'
  eval('connection.set_session'+arguements_string)

def execute_statement(connection, cursor, statement_type, sql):
  try:
    if statement_type == 'query':
      cursor.execute(sql)
      if cursor.rowcount == 0:
        print('No Result Found')
      else:
        for row in cursor:
          print(row)
    elif statement_type == 'update':
      cursor.execute(sql)
      print(cursor.rowcount)
    else:
      if sql == 'commit':
        connection.commit()
      elif sql == 'rollback':
        connection.rollback()
      elif 'set' in sql:
        if 'session' in sql:
          sql = sql[12:]
          set_session_property(connection, sql)
        else:
          sql = sql[4:]
          property, value = sql.split(' ')
          set_property(connection, property, value)
      else:
        raise Exception('Invalid Transaction Statement Found')
  except Exception as e:
    raise Exception(e)




def execute_transaction_statements(statements, port):
  try:
    connection = create_connection(port)
    cursor = connection.cursor()
    i = 0
    while i < len(statements):
      execute_statement(connection, cursor, statements[i], statements[i + 1])
      i += 2
    cursor.close()
    connection.close()
  except Exception as e:
    print(str(e).strip())

if __name__ == '__main__':
  port = sys.argv[1]
  statements = sys.argv[2:]

  execute_transaction_statements(statements, port)
