from django.db import models

class Singer(models.Model):
  class Meta():
    db_table = 'singers'
  singer_id = models.IntegerField(primary_key=True)
  first_name = models.CharField(max_length=30)
  last_name = models.CharField(max_length=30)
