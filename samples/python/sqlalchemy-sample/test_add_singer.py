from model import Singer
from sample import add_singer
from datetime import datetime

singer = Singer()
singer.id = "123-456-789"
singer.first_name = "Myfirstname"
singer.last_name = "Mylastname"
singer.active = True
# Manually set a created_at value, as we otherwise do not know which value to
# add to the mock server.
singer.created_at = datetime.fromisoformat("2011-11-04T00:05:23.123456+00:00"),
add_singer(singer)
