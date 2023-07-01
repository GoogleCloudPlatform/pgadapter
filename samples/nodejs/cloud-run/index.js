const express = require('express');
const { Client } = require('pg');
const app = express();
const client = new Client({
  host: 'localhost',
  port: 5432,
  database: `projects/${process.env.SPANNER_PROJECT}/instances/${process.env.SPANNER_INSTANCE}/databases/${process.env.SPANNER_DATABASE}`,
});
const port = parseInt(process.env.PORT) || 8080;

client.connect().then(() => {
  app.listen(port, () => {
    console.log(`helloworld: listening on port ${port}`);
  });
});

app.get('/', async (req, res) => {
  // Execute a query on Cloud Spanner that returns a greeting and return that to the user.
  const greeting = await client.query("select 'Hello world! from Cloud Spanner PostgreSQL' as hello");
  res.send(greeting.rows[0].hello + '\n');
});
