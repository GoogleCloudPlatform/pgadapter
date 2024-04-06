# Copyright 2024 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from samples import create_tables
import socket
import time
import unittest.mock
from io import StringIO
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


class SampleTest(unittest.TestCase):
    container = None

    @classmethod
    def setUpClass(cls):
        cls.container = (
            DockerContainer("gcr.io/cloud-spanner-pg-adapter/"
                            "pgadapter-emulator")
            .with_exposed_ports(5432))
        cls.container.start()
        # Wait until PGAdapter has started and is listening on the exposed port.
        wait_for_logs(cls.container, "PostgreSQL version:")
        port = cls.container.get_exposed_port(5432)
        _wait_for_port(port=int(port))

    @classmethod
    def tearDownClass(cls):
        if cls.container is not None:
            cls.container.stop()

    @unittest.mock.patch('sys.stdout', new_callable=StringIO)
    def test_run_samples(self, mock_stdout):
        create_tables.create_tables("localhost", self.__class__.container
                                    .get_exposed_port(5432), "example-db")
        self.assertEqual(
            "Created Singers & Albums tables in database: [example-db]\n",
            mock_stdout.getvalue())


def _wait_for_port(port: int, poll_interval: float = 0.1, timeout: float = 5.0):
    start = time.time()
    while True:
        try:
            with socket.create_connection(("localhost", port), timeout=timeout):
                break
        except OSError:
            duration = time.time() - start
            if timeout and duration > timeout:
                raise TimeoutError("container did not listen on port {} in {} "
                                   "seconds".format(port, timeout))
            time.sleep(poll_interval)


if __name__ == '__main__':
    unittest.main()
