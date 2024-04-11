package golang_snippets

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestSamples(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		AlwaysPullImage: true,
		Image:           "gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator",
		ExposedPorts:    []string{"5432/tcp"},
		WaitingFor:      wait.ForListeningPort("5432/tcp"),
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.AutoRemove = true
		},
	}
	pg, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start PGAdapter: %v", err)
	}
	defer pg.Terminate(ctx)
	host, err := pg.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get host: %v", err)
	}
	mappedPort, err := pg.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}
	port := mappedPort.Int()

	db := "example-db"
	if err := createTables(host, port, db); err != nil {
		t.Fatalf("creating tables failed: %v", err)
	}
	if err := createConnection(host, port, db); err != nil {
		t.Fatalf("creating connection failed: %v", err)
	}
	if err := writeDataWithDml(host, port, db); err != nil {
		t.Fatalf("writing data with dml failed: %v", err)
	}
	if err := writeDataWithDmlBatch(host, port, db); err != nil {
		t.Fatalf("writing data with dml batch failed: %v", err)
	}
	if err := writeDataWithCopy(host, port, db); err != nil {
		t.Fatalf("writing data with copy failed: %v", err)
	}
}
