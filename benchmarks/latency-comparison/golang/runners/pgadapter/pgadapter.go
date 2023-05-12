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
	"github.com/docker/docker/api/types/container"
	"os"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/oauth2/google"
)

var pgadapter testcontainers.Container

// StartPGAdapter starts a PGAdapter instance in a Docker container using the default
// credentials of the environment. Returns the host port number it is listening on.
// Connect to `localhost:port` using a PostgreSQL driver.
//
// project: The Google Cloud Project ID where the Cloud Spanner instance has been created (e.g. `my-project`).
// instance: The Cloud Spanner instance ID (e.g. `my-instance`).
func StartPGAdapter(ctx context.Context, project, instance string) (port int, cleanup func(), err error) {
	credentials, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		return 0, func() {}, err
	}
	return StartPGAdapterWithCredentials(ctx, project, instance, credentials)
}

// StartPGAdapterWithCredentials starts a PGAdapter instance using the specified credentials
// in a Docker container and returns the host port number it is listening on.
// Connect to `localhost:port` using a PostgreSQL driver.
//
// project: The Google Cloud Project ID where the Cloud Spanner instance has been created (e.g. `my-project`).
// instance: The Cloud Spanner instance ID (e.g. `my-instance`).
// credentials: The Google credentials to use.
func StartPGAdapterWithCredentials(ctx context.Context, project, instance string, credentials *google.Credentials) (port int, cleanup func(), err error) {
	if credentials == nil {
		return 0, func() {}, fmt.Errorf("credentials cannot be nil")
	}
	if credentials.JSON == nil || len(credentials.JSON) == 0 {
		return 0, func() {}, fmt.Errorf("only JSON based credentials are supported")
	}
	credentialsFile, err := os.CreateTemp(os.TempDir(), "pgadapter-credentials")
	if err != nil {
		return 0, func() {}, err
	}
	defer os.RemoveAll(credentialsFile.Name())
	if _, err = credentialsFile.Write(credentials.JSON); err != nil {
		return 0, func() {}, err
	}
	req := testcontainers.ContainerRequest{
		Image:           "gcr.io/cloud-spanner-pg-adapter/pgadapter",
		AlwaysPullImage: true,
		HostConfigModifier: func(config *container.HostConfig) {
			config.AutoRemove = true
			config.Binds = []string{"/tmp:/tmp"}
		},
		Cmd: []string{
			"-p", project,
			"-i", instance,
			"-x",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      credentialsFile.Name(),
				ContainerFilePath: "/credentials.json",
				FileMode:          700,
			},
		},
		Env:          map[string]string{"GOOGLE_APPLICATION_CREDENTIALS": "/credentials.json"},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForExposedPort(),
	}
	pgadapter, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return 0, func() {}, err
	}
	mappedPort, err := pgadapter.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return 0, stopPGAdapter, err
	}
	time.Sleep(time.Second)
	return mappedPort.Int(), stopPGAdapter, nil
}

// stopPGAdapter stops the PGAdapter Docker container.
func stopPGAdapter() {
	if pgadapter == nil {
		return
	}
	if pgadapter.IsRunning() {
		if err := pgadapter.Terminate(context.Background()); err != nil {
			fmt.Printf("failed to terminate PGAdapter: %v\n", err)
		}
	}
}
