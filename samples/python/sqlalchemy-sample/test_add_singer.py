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
