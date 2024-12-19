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

from samples import create_tables, create_connection, write_data_with_dml, \
    write_data_with_dml_batch, \
    write_data_with_copy, query_data, query_data_with_parameter, add_column, \
    ddl_batch, partitioned_dml, update_data_with_copy, \
    update_data_with_transaction, tags, read_only_transaction, data_boost, \
    statement_timeout, query_data_with_new_column
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
        host = "localhost"
        port = self.__class__.container.get_exposed_port(5432)
        db = "example-db"
        create_tables.create_tables(host, port, db)
        self.assertEqual(
            "Created Singers & Albums tables in database: [example-db]\n",
            mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        create_connection.create_connection(host, port, db)
        self.assertEqual(
            "Greeting from Cloud Spanner PostgreSQL: Hello world!\n",
            mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        write_data_with_dml.write_data_with_dml(host, port, db)
        self.assertEqual("4 records inserted\n", mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        write_data_with_dml_batch.write_data_with_dml_batch(host, port, db)
        self.assertEqual("3 records inserted\n", mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        write_data_with_copy.write_data_with_copy(host, port, db)
        self.assertEqual("Copied 5 singers\nCopied 5 albums\n",
                         mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        query_data.query_data(host, port, db)
        self.assertEqual("(1, 2, 'Go, Go, Go')\n"
                         "(2, 2, 'Forever Hold Your Peace')\n"
                         "(1, 1, 'Total Junk')\n"
                         "(2, 1, 'Green')\n"
                         "(2, 3, 'Terrified')\n", mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        query_data_with_parameter.query_data_with_parameter(host, port, db)
        self.assertEqual("(12, 'Melissa', 'Garcia')\n",
                         mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        statement_timeout.query_with_timeout(host, port, db)
        self.assertEqual("", mock_stdout.getvalue().lower())
        reset_mock_stdout(mock_stdout)

        add_column.add_column(host, port, db)
        self.assertEqual("Added marketing_budget column\n",
                         mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        ddl_batch.ddl_batch(host, port, db)
        self.assertEqual("Added venues and concerts tables\n",
                         mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        update_data_with_copy.update_data_with_copy(host, port, db)
        self.assertEqual("Updated 2 albums\n",
                         mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        query_data_with_new_column.query_data_with_new_column(host, port, db)
        self.assertEqual("(1, 1, 100000)\n"
                         "(1, 2, None)\n"
                         "(2, 1, None)\n"
                         "(2, 2, 500000)\n"
                         "(2, 3, None)\n", mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        update_data_with_transaction.update_data_with_transaction(
            host, port, db)
        self.assertEqual(
            "Transferred marketing budget from Album 2 to Album 1\n",
            mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        tags.tags(host, port, db)
        self.assertEqual("Reduced marketing budget\n",
                         mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        read_only_transaction.read_only_transaction(host, port, db)
        self.assertEqual("(1, 1, 'Total Junk')\n"
                         "(1, 2, 'Go, Go, Go')\n"
                         "(2, 1, 'Green')\n"
                         "(2, 2, 'Forever Hold Your Peace')\n"
                         "(2, 3, 'Terrified')\n"
                         "(2, 2, 'Forever Hold Your Peace')\n"
                         "(1, 2, 'Go, Go, Go')\n"
                         "(2, 1, 'Green')\n"
                         "(2, 3, 'Terrified')\n"
                         "(1, 1, 'Total Junk')\n",
                         mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        data_boost.data_boost(host, port, db)
        self.assertIn("(17, 'Ethan', 'Miller')\n",
                      mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)

        partitioned_dml.execute_partitioned_dml(host, port, db)
        self.assertEqual("Updated at least 3 albums\n",
                         mock_stdout.getvalue())
        reset_mock_stdout(mock_stdout)


def reset_mock_stdout(mock_stdout):
    mock_stdout.truncate(0)
    mock_stdout.seek(0)


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
