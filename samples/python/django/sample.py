import os
import sys

import django
from django.db import connection
def createtablesIfNotExists():
  file = open('create_data_model.sql', 'r')
  ddl_statements = file.read()
  with connection.cursor() as cursor:
    cursor.execute(ddl_statements)


if __name__ == "__main__":

  try:
    #setting up django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'setting')
    django.setup()
    print('Django Setup Created')
    #creating the tables if they don't exist
    createtablesIfNotExists()
    print('Tables corresponding to data models created')
    #importing the models
    from sample_app.model import Singer, Album, Track, Concert, Venue


  except Exception as e:
    print(e)
    sys.exit(1)