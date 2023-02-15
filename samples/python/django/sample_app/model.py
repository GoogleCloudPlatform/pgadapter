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
from django.db.models import Q, F
from django.contrib.postgres.fields import JSONField

class BaseModel(models.Model):
  class Meta():
    abstract = True
  created_at = models.DateTimeField()
  updated_at = models.DateTimeField()

class Singer(BaseModel):
  class Meta():
    db_table = 'singers'

  id = models.CharField(primary_key=True, null=False)
  first_name = models.CharField()
  last_name = models.CharField(null=False)
  full_name = models.CharField(null=False)
  active = models.BooleanField()

class Album(BaseModel):
  class Meta():
    db_table = 'albums'

  id = models.CharField(primary_key=True, null=False)
  title = models.CharField(null=False)
  marketing_budget = models.DecimalField()
  release_date = models.DateField()
  cover_picture = models.BinaryField()
  singer = models.ForeignKey(Singer, on_delete=models.DO_NOTHING)

class Track(BaseModel):
  class Meta():
    db_table = 'tracks'

  # Here, track_id is a column that is supposed to be primary key by Django.
  # But id column will just have a unique index in the actual table.
  # In the actual table, (id, track_number) will be the primary key.
  # This is done because Django doesn't support composite primary keys,
  # but we need to have a composite primary key due to the fact that
  # the "tracks" table is interleaved in "albums".

  track_id = models.CharField(primary_key=True, null=False)
  album = models.ForeignKey(Album, on_delete=models.DO_NOTHING, db_column='id')
  track_number = models.BigIntegerField(null=False)
  title = models.CharField(null=False)
  sample_rate = models.FloatField()


class Venue(BaseModel):
  class Meta():
    db_table = 'venues'
  id = models.CharField(primary_key=True, null=False)
  name = models.CharField(null=False)
  description = models.JSONField()


class Concert(BaseModel):
  class Meta():
    db_table = 'concerts'
    constraints = [models.CheckConstraint(check = Q(end_time__gte=F('start_time')), name='chk_end_time_after_start_time' )]
  id = models.CharField(primary_key=True, null=False)
  venue = models.ForeignKey(Venue, on_delete=models.DO_NOTHING)
  singer = models.ForeignKey(Singer, on_delete=models.DO_NOTHING)
  name = models.CharField(null=False)
  start_time = models.DateTimeField(null=False)
  end_time = models.DateTimeField(null=False)

