from django.db import models
from django.contrib.auth.models import UserManager

class Singer(models.Model):
  class Meta():
    db_table = 'singers'
  singerid = models.IntegerField(primary_key=True)
  firstname = models.CharField(max_length=30)
  lastname = models.CharField(max_length=30)
