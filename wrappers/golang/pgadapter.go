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

package pgadapter

import (
	"context"
	"fmt"
	"os"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/oauth2/google"
)

type Config struct {
	// Project specifies the Google Cloud project that PGAdapter should connect to.
	// This value is optional. If it is not set, clients that connect to the PGAdapter
	// instance must use a fully qualified database name when connecting.
	Project string
	// Instance specifies the Google Cloud Spanner instance that PGAdapter should
	// connect to. This value is optional. If it is not set, clients that connect
	// to the PGAdapter instance must use a fully qualified database name when connecting.
	Instance string
	// Database specifies the Google Cloud Spanner database that PGAdapter should
	// connect to. This value is optional. If it is set, any database name in the
	// connection URL of a PostgreSQL client will be ignored, and PGAdapter will always
	// connect to this specific database.
	// If Database is not set, then PGAdapter will use the database name from the
	// PostgreSQL connection URL.
	Database string
	// CredentialsFile is the user account or service account key file that PGAdapter
	// should use to connect to Cloud Spanner.
	// This value is optional. If it is not set, PGAdapter will use the default
	// credentials in the runtime environment.
	CredentialsFile string

	// Port is the host TCP port where PGAdapter should listen for incoming connections.
	// The port must not be in use by any other process.
	// Use port 0 to dynamically assign an available port to PGAdapter.
	Port int
}

type PGAdapter struct {
	container testcontainers.Container
	port      int
	stopped   bool
}

// Start starts a PGAdapter instance using the specified Config
// in a Docker container and returns a reference to a PGAdapter
// instance.
//
// Call GetHostPort() to get the port where PGAdapter is listening for
// incoming connections.
func Start(ctx context.Context, config Config) (pgadapter *PGAdapter, err error) {
	var credentialsFile string
	if config.CredentialsFile == "" {
		credentials, err := google.FindDefaultCredentials(ctx)
		if credentials == nil {
			return nil, fmt.Errorf("credentials cannot be nil")
		}
		if credentials.JSON == nil || len(credentials.JSON) == 0 {
			return nil, fmt.Errorf("only JSON based credentials are supported")
		}
		f, err := os.CreateTemp(os.TempDir(), "pgadapter-credentials")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(f.Name())
		if _, err = f.Write(credentials.JSON); err != nil {
			return nil, err
		}
		credentialsFile = f.Name()
	} else {
		credentialsFile = config.CredentialsFile
	}
	if err != nil {
		return nil, err
	}
	var cmd []string
	if config.Project != "" {
		cmd = append(cmd, "-p", config.Project)
	}
	if config.Instance != "" {
		cmd = append(cmd, "-i", config.Instance)
	}
	if config.Database != "" {
		cmd = append(cmd, "-d", config.Database)
	}
	cmd = append(cmd, "-x")
	req := testcontainers.ContainerRequest{
		AlwaysPullImage: true,
		Image:           "gcr.io/cloud-spanner-pg-adapter/pgadapter",
		Cmd:             cmd,
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      credentialsFile,
				ContainerFilePath: "/credentials.json",
				FileMode:          700,
			},
		},
		Env:          map[string]string{"GOOGLE_APPLICATION_CREDENTIALS": "/credentials.json"},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForExposedPort(),
	}
	pgadapter = &PGAdapter{}
	pgadapter.container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	mappedPort, err := pgadapter.container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return pgadapter, err
	}
	pgadapter.port = mappedPort.Int()
	return pgadapter, nil
}

func (pgadapter *PGAdapter) GetHostPort() (int, error) {
	if pgadapter == nil {
		return 0, fmt.Errorf("nil reference")
	}
	if pgadapter.container == nil || pgadapter.container.IsRunning() || pgadapter.port == 0 {
		return 0, fmt.Errorf("PGAdapter has not been started successfully")
	}
	return pgadapter.port, nil
}

// Stop stops the PGAdapter instance.
func (pgadapter *PGAdapter) Stop() error {
	if pgadapter == nil {
		return fmt.Errorf("nil reference")
	}
	if pgadapter.stopped {
		return nil
	}
	if pgadapter.container.IsRunning() {
		return pgadapter.container.Terminate(context.Background())
	}
	return nil
}
