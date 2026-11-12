/*
Copyright 2026 Google LLC
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

const assert = require('node:assert');
const { spawn } = require('node:child_process');
const net = require('node:net');
const { GenericContainer, PullPolicy } = require('testcontainers');

async function getAvailablePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, () => {
      const port = server.address().port;
      server.close((err) => {
        if (err) reject(err);
        else resolve(port);
      });
    });
  });
}

async function runSmokeTest() {
  console.log('Starting PGAdapter + Spanner Emulator container...');
  const container = await new GenericContainer('gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator')
    .withPullPolicy(PullPolicy.alwaysPull())
    .withExposedPorts(5432)
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(5432);
  const appPort = await getAvailablePort();

  console.log(`PGAdapter running at ${host}:${port}. Launching index.js on port ${appPort}...`);

  const appProcess = spawn('node', ['index.js'], {
    env: {
      ...process.env,
      PGADAPTER_HOST: host,
      PGADAPTER_PORT: port.toString(),
      PORT: appPort.toString(),
      SPANNER_PROJECT: 'my-project',
      SPANNER_INSTANCE: 'my-instance',
      SPANNER_DATABASE: 'my-database',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  try {
    await new Promise((resolve, reject) => {
      appProcess.stdout.on('data', (data) => {
        const message = data.toString();
        process.stdout.write(message);
        if (message.includes('listening on port')) {
          resolve();
        }
      });
      appProcess.stderr.on('data', (data) => {
        process.stderr.write(data.toString());
      });
      appProcess.on('error', reject);
      appProcess.on('exit', (code) => {
        if (code !== null && code !== 0) {
          reject(new Error(`Application exited prematurely with code ${code}`));
        }
      });
    });

    console.log(`Querying http://localhost:${appPort}/ ...`);
    const response = await fetch(`http://localhost:${appPort}/`);
    const body = await response.text();

    assert.strictEqual(response.status, 200, `Expected status 200, got ${response.status}`);
    assert.strictEqual(
      body,
      'Hello world! from Cloud Spanner PostgreSQL using Node.js\n',
      `Unexpected response body: ${body}`
    );
    console.log('Smoke test passed successfully!');
  } finally {
    appProcess.kill();
    await container.stop();
  }
}

runSmokeTest().catch((error) => {
  console.error('Smoke test failed:', error);
  process.exit(1);
});
