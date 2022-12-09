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

from django.db import models

class Singer(models.Model):
  class Meta():
    db_table = 'singers'
  singerid = models.IntegerField(primary_key=True)
  firstname = models.CharField(max_length=30)
  lastname = models.CharField(max_length=30)

class all_types(models.Model):
  class Meta():
    db_table = 'all_types'
  col_bigint = models.BigIntegerField(primary_key=True)
  col_bool = models.BooleanField()
  col_bytea = models.BinaryField(null=True)
  col_float8 = models.FloatField()
  col_int = models.IntegerField()
  col_numeric = models.DecimalField(max_digits=15, decimal_places=5)
  col_timestamptz = models.DateTimeField()
  col_date = models.DateField()
  col_varchar = models.CharField(null=True)
