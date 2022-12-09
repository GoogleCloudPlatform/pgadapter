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
class Singer(models.Model):
  class Meta():
    db_table = 'singers'
  id = models.IntegerField(primary_key=True, null=False)
  first_name = models.CharField()
  last_name = models.CharField(null=False)

  @property
  def _get_full_name(self):
    return '{first_name} {last_name}'.format(first_name=self.first_name, last_name=self.last_name)
  full_name = property(_get_full_name())
  active = models.BooleanField()
  createdAt = models.DateTimeField()
  updatedAt = models.DateTimeField()

class Album(models.Model):
  class Meta():
    db_table = 'albums'
  id = models.CharField(primary_key=True, null=False)
  title = models.CharField(null=False)
  marketing_budget = models.DecimalField()
  release_date = models.DateField()
  cover_picture = models.BinaryField()
  singer_id = models.ForeignKey(Singer, on_delete=models.CASCADE())
  createdAt = models.DateTimeField()
  updatedAt = models.DateTimeField()

class Track(models.Model):
  class Meta():
    db_table = 'tracks'
    unique_together = ('id', 'track_number')
  id = models.ForeignKey(Album, on_delete=models.CASCADE())
  track_number = models.BigIntegerField(null=False)
  title = models.CharField(null=False)
  sample_rate = models.DecimalField()
  createdAt = models.DateTimeField()
  updatedAt = models.DateTimeField()

class Venue(models.Model):
  class Meta():
    db_table = 'venues'
  id = models.CharField(primary_key=True, null=False)
  name = models.CharField(null=False)
  description = models.CharField(null=False)
  createdAt = models.DateTimeField()
  updatedAt = models.DateTimeField()

class Concert(models.Model):
  class Meta():
    db_table = 'concerts'
    constraints = [models.CheckConstraint(check = Q(end_time__gte=F('start_time')), name='chk_end_time_after_start_time' )]
  id = models.CharField(primary_key=True, null=False)
  venue_id = models.ForeignKey(Venue)
  singer_id = models.ForeignKey(Singer)
  name = models.CharField(null=False)
  start_time = models.DateTimeField(null=False)
  end_time = models.DateTimeField(null=False)
  createdAt = models.DateTimeField()
  updatedAt = models.DateTimeField()
