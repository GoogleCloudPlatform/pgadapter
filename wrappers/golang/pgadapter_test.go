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
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestStart(t *testing.T) {
	home, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("failed to get cache dir: %v", err)
	}
	downloadLocation := filepath.Join(home, "pgadapter-downloads")
	for i, config := range []Config{
		{ExecutionEnvironment: &Docker{}},
		{ExecutionEnvironment: &Docker{AlwaysPullImage: true}},
		{ExecutionEnvironment: &Docker{}, Version: "v0.20.0"},
		{ExecutionEnvironment: &Java{}},
		{ExecutionEnvironment: &Java{DownloadSettings: DownloadSettings{DownloadLocation: downloadLocation}}},
		{ExecutionEnvironment: &Java{DownloadSettings: DownloadSettings{DisableAutomaticDownload: true, DownloadLocation: filepath.Join(downloadLocation, "pgadapter-latest")}}},
		{ExecutionEnvironment: &Java{}, Version: "v0.20.0"},
	} {
		pgadapter, err := Start(context.Background(), config)
		if err != nil {
			t.Fatalf("%d: failed to start PGAdapter: %v", i, err)
		}
		port, err := pgadapter.GetHostPort()
		if err != nil {
			t.Fatalf("%d: failed to get port from PGAdapter: %v", i, err)
		}
		if port == 0 {
			t.Fatalf("%d: Port should be non-zero: %v", i, err)
		}

		servAddr := fmt.Sprintf("localhost:%d", port)
		tcpAddr, err := net.ResolveTCPAddr("tcp", servAddr)
		if err != nil {
			t.Fatalf("%d: ResolveTCPAddr failed: %v", i, err)
		}
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			t.Fatalf("%d: Dial failed: %v", i, err)
		}
		// Send an invalid startup message. This should validate that we are talking to
		// a working PGAdapter instance.
		err = binary.Write(conn, binary.BigEndian, int32(8))
		if err != nil {
			t.Fatalf("Write length failed: %v", err)
		}
		err = binary.Write(conn, binary.BigEndian, int32(9999))
		if err != nil {
			t.Fatalf("Write startup message failed: %v", err)
		}
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 256)
		for {
			n, err := conn.Read(tmp)
			if err != nil {
				if err != io.EOF {
					t.Fatalf("%d: Read failed: %v", i, err)
				}
				break
			}
			buf = append(buf, tmp[:n]...)
		}
		if g, w := len(buf), 42; g != w {
			t.Fatalf("Received unexpected buffer length\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := string(buf[0]), "E"; g != w {
			t.Fatalf("Received unexpected server response type\nGot:  %v\nWant: %v", g, w)
		}
		// The encoded message length does not include the message type, which is 1 byte long.
		if g, w := binary.BigEndian.Uint32(buf[1:5]), uint32(36); g != w {
			t.Fatalf("Received unexpected server response length\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := string(buf[20:35]), "Unknown message"; g != w {
			t.Fatalf("Received unexpected error message\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := string(buf[37]), "X"; g != w {
			t.Fatalf("Missing terminate message\nGot:  %v\nWant: %v", g, w)
		}
		if g, w := binary.BigEndian.Uint32(buf[38:]), uint32(4); g != w {
			t.Fatalf("Received unexpected server response length\nGot:  %v\nWant: %v", g, w)
		}
		err = pgadapter.Stop(context.Background())
		if err != nil {
			t.Fatalf("failed to stop PGAdapter: %v", err)
		}
	}
}

func TestCommand(t *testing.T) {
	for i, exec := range []struct {
		conf    Config
		want    string
		wantErr error
	}{
		{
			Config{ExecutionEnvironment: &Docker{}},
			"docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter ",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{KeepContainer: true}},
			"docker run -d -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter ",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{}, Port: 9999},
			"docker run -d --rm -p 9999:5432 gcr.io/cloud-spanner-pg-adapter/pgadapter ",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{}, Version: "0.20.0"},
			"docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter:0.20.0 ",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{}, Project: "my-project"},
			"docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter -p my-project",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{}, Instance: "my-instance"},
			"",
			fmt.Errorf("you must also set a project when you set an instance"),
		},
		{
			Config{ExecutionEnvironment: &Docker{}, Project: "my-project", Instance: "my-instance"},
			"docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter -p my-project -i my-instance",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{}, Database: "my-database"},
			"",
			fmt.Errorf("you must also set an instance when you set a database"),
		},
		{
			Config{ExecutionEnvironment: &Docker{}, Project: "my-project", Instance: "my-instance", Database: "my-database"},
			"docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter -p my-project -i my-instance -d my-database",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{}, RequireAuthentication: true},
			"docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter -a",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{}, CredentialsFile: "/path/to/credentials.json"},
			"docker run -d --rm -p 5432 -v /path/to/credentials.json:/credentials.json:ro gcr.io/cloud-spanner-pg-adapter/pgadapter -c /credentials.json",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{}, CredentialsFile: "/path/to/credentials.json", RequireAuthentication: true},
			"",
			fmt.Errorf("cannot set both a credentials file and require authentication"),
		},
		{
			Config{ExecutionEnvironment: &Docker{}, MinSessions: 100, MaxSessions: 50},
			"",
			fmt.Errorf("MaxSessions must be >= MinSessions"),
		},
		{
			Config{ExecutionEnvironment: &Docker{}, MinSessions: -1},
			"",
			fmt.Errorf("MinSessions must be non-negative"),
		},
		{
			Config{ExecutionEnvironment: &Docker{}, MaxSessions: -1},
			"",
			fmt.Errorf("MaxSessions must be non-negative"),
		},
		{
			Config{ExecutionEnvironment: &Docker{}, NumChannels: -1},
			"",
			fmt.Errorf("NumChannels must be non-negative"),
		},
		{
			Config{ExecutionEnvironment: &Docker{}, MinSessions: 50, MaxSessions: 100},
			"docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter -r minSessions=50;maxSessions=100;",
			nil,
		},
		{
			Config{ExecutionEnvironment: &Docker{}, NumChannels: 10},
			"docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter -r numChannels=10;",
			nil,
		},
	} {
		cmd, err := exec.conf.ExecutionEnvironment.Command(exec.conf)
		if (exec.wantErr == nil && err != nil) || (exec.wantErr != nil && err == nil) || (exec.wantErr != nil && err != nil && exec.wantErr.Error() != err.Error()) {
			t.Errorf("%d: error mismatch\n Got: %v\nWant: %v", i, err, exec.wantErr)
		}
		if g, w := cmd, exec.want; g != w {
			t.Errorf("%d: command mismatch\n Got: %v\nWant: %v", i, g, w)
		}
	}
}
