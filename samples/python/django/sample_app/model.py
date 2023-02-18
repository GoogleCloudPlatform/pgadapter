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

from django.db import models
from django.db.models import Q, F

class BaseModel(models.Model):
  class Meta:
    abstract = True
  created_at = models.DateTimeField()
  updated_at = models.DateTimeField()

class Singer(BaseModel):
  class Meta:
    db_table = 'singers'

  id = models.CharField(primary_key=True, null=False)
  first_name = models.CharField()
  last_name = models.CharField(null=False)
  full_name = models.CharField(null=False)
  active = models.BooleanField()

class Album(BaseModel):
  class Meta:
    db_table = 'albums'

  id = models.CharField(primary_key=True, null=False)
  title = models.CharField(null=False)
  marketing_budget = models.DecimalField()
  release_date = models.DateField()
  cover_picture = models.BinaryField()
  singer = models.ForeignKey(Singer, on_delete=models.DO_NOTHING)

class Track(BaseModel):
  class Meta:
    db_table = 'tracks'

  # Track is interleaved in the parent table Album. Cloud Spanner requires that
  # a child table includes all the primary key columns of the parent table in
  # its primary key, followed by any primary key column(s) of the child table.
  # This means that a child table will always have a composite primary key.
  # Composite primary keys are however not supported by Django. The workaround
  # that we apply here is to create a separate field `track_id` and tell Django
  # that this is the primary key of the table. The actual schema definition
  # for the `tracks` table does not have this as its primary key. Instead, the
  # primary key of this table is (`id`, `track_number`). In addition, there is a
  # unique index defined on `track_id` to ensure that Track rows can efficiently
  # be retrieved using the identifier that Django thinks is the primary key of
  # this table.
  # See create_data_model.sql file in this directory for the table definition.
  track_id = models.CharField(primary_key=True, null=False)
  album = models.ForeignKey(Album, on_delete=models.DO_NOTHING, db_column='id')
  track_number = models.BigIntegerField(null=False)
  title = models.CharField(null=False)
  sample_rate = models.FloatField()


class Venue(BaseModel):
  class Meta:
    db_table = 'venues'
  id = models.CharField(primary_key=True, null=False)
  name = models.CharField(null=False)
  description = models.JSONField()


class Concert(BaseModel):
  class Meta:
    db_table = 'concerts'
    constraints = [models.CheckConstraint(
      check = Q(end_time__gte=F('start_time')),
      name='chk_end_time_after_start_time' )]
  id = models.CharField(primary_key=True, null=False)
  venue = models.ForeignKey(Venue, on_delete=models.DO_NOTHING)
  singer = models.ForeignKey(Singer, on_delete=models.DO_NOTHING)
  name = models.CharField(null=False)
  start_time = models.DateTimeField(null=False)
  end_time = models.DateTimeField(null=False)

