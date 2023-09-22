/*
Copyright 2023 Google LLC
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

const express = require('express');
const { Client } = require('pg');
const app = express();

// Get Cloud Spanner settings.
const project = process.env.SPANNER_PROJECT || "my-project";
const instance = process.env.SPANNER_INSTANCE || "my-instance";
const database = process.env.SPANNER_DATABASE || "my-database";

// Get PGAdapter settings.
const pgadapterHost = process.env.PGADAPTER_HOST || "localhost";
const pgadapterPort = parseInt(process.env.PGADAPTER_PORT || "5432");

// Create a pg Client that connects to PGAdapter and uses a fully qualified
// database name for the database that it should connect to.
const client = new Client({
  host: pgadapterHost,
  port: pgadapterPort,
  database: `projects/${project}/instances/${instance}/databases/${database}`,
});
const port = parseInt(process.env.PORT) || 8080;

client.connect().then(() => {
  app.listen(port, () => {
    console.log(`helloworld: listening on port ${port}`);
  });
});

app.get('/', async (req, res) => {
  // Execute a query on Cloud Spanner that returns a greeting and return that to the user.
  const greeting = await client.query("select 'Hello world! from Cloud Spanner PostgreSQL using Node.js' as hello");
  res.send(greeting.rows[0].hello + '\n');
});
