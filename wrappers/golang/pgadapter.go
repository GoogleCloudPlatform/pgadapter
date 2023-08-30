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
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/oauth2/google"
)

// ExecutionEnvironment is the common interface for the supported execution environments
// for PGAdapter. Currently, Java and Docker are supported.
type ExecutionEnvironment interface {
	// Command returns the command and arguments that is the equivalent of
	// this ExecutionEnvironment. Use the output of this command to get the
	// equivalent command for starting PGAdapter manually.
	//
	// Example return value for Java:
	//
	// java -jar pgadapter.jar -p my-project -i my-instance -d my-database -c /path/to/credentials.json
	//
	// Example return value for Docker:
	//
	// docker run
	// 	 -d \
	//   -p 5432 \
	//   gcr.io/cloud-spanner-pg-adapter/pgadapter \
	//   -p my-project -i my-instance -d my-database \
	//   -c /credentials.json -x
	Command(config Config) (string, error)

	supportsFixedPort() bool
}

const dockerImage = "gcr.io/cloud-spanner-pg-adapter/pgadapter"

// Docker contains the specific configuration for running PGAdapter in a test Docker container.
// This Docker execution environment can be used for tests and development. You should however
// not use this Docker execution environment in production, as it uses a test container.
// Instead, you should package both your application and PGAdapter in the same Docker container
// if you want to use Docker in production, or set up PGAdapter as a side-car process in your
// Kubernetes cluster.
type Docker struct {
	// Set AlwaysPullImage to true to ensure that the latest image of PGAdapter is
	// pulled before it is started.
	AlwaysPullImage bool
	// Set KeepContainer to keep the container after it has stopped. The Docker
	// container is started with the '--rm' option by default, unless this option
	// is set.
	KeepContainer bool
}

func (*Docker) supportsFixedPort() bool {
	return false
}

func (docker *Docker) Command(config Config) (string, error) {
	args, err := config.toArguments()
	if err != nil {
		return "", err
	}
	cmd := "docker run -d "
	if !docker.KeepContainer {
		cmd += "--rm "
	}
	if config.Port > 0 {
		cmd += fmt.Sprintf("-p %d:5432 ", config.Port)
	} else {
		cmd += "-p 5432 "
	}
	if config.CredentialsFile != "" {
		cmd += "-v " + config.CredentialsFile + ":/credentials.json:ro "
	}
	image := dockerImage
	if config.Version != "" {
		image += ":" + config.Version
	}
	cmd += image + " "
	cmd += strings.Join(args, " ")
	return cmd, nil
}

// DownloadSettings determines where the PGAdapter jar will be downloaded.
type DownloadSettings struct {
	// DownloadLocation specifies the location where the PGAdapter jar should be
	// stored. It will use the directory that is returned by os.UserCacheDirectory()
	// if no location is specified.
	DownloadLocation string

	// DisableAutomaticDownload can be used to disable the automatic download of the
	// PGAdapter jar. Set this if you do not want this wrapper to automatically download
	// the PGAdapter jar, for example if the machine does not have permission to write
	// to the local file system.
	DisableAutomaticDownload bool
}

// Java holds the specific configuration for running PGAdapter as a Java application.
type Java struct {
	DownloadSettings DownloadSettings
}

func (*Java) supportsFixedPort() bool {
	return true
}

func (java *Java) Command(config Config) (string, error) {
	args, err := config.toArguments()
	if err != nil {
		return "", err
	}
	cmd := "java -jar " + java.DownloadSettings.DownloadLocation
	if !strings.HasSuffix(cmd, "/") {
		cmd += "/"
	}
	cmd += "pgadapter.jar "
	cmd += strings.Join(args, " ")
	return cmd, nil
}

type Config struct {
	// ExecutionEnvironment determines the execution environment that is used to start
	// PGAdapter.
	//
	// Java starts PGAdapter on localhost as a Java application. This requires Java to be
	// installed on the local system. See for example https://adoptium.net/installation/
	// for information on how to install an open-source Java distribution on your system.
	//
	// Java is the recommended setting for production environments, as it means that your
	// application and PGAdapter are both running on the same host, and the network traffic
	// does not need to pass over the host-to-Docker network bridge. Note that it is perfectly
	// OK to run both your application and PGAdapter in the same Docker container.
	//
	// The Java process is automatically shut down when your application shuts down.
	//
	// Docker starts PGAdapter in a test container instance. This is recommended for testing
	// and development, but not for production, as it means that the network communication
	// between your application and PGAdapter crosses the host-to-Docker network bridge.
	// This network interface is slow compared to network communication directly on localhost.
	//
	// This variable must be set.
	ExecutionEnvironment ExecutionEnvironment

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
	// Set RequireAuthentication to instruct PGAdapter to require PostgreSQL clients
	// to authenticate when connecting. When enabled, clients must supply the Google
	// Cloud credentials that should be used in the password field of the startup
	// message. See https://github.com/GoogleCloudPlatform/pgadapter/docs/authentication.md
	// for more information.
	RequireAuthentication bool

	// The following settings are for the internal session pool that PGAdapter uses
	// for communication with Cloud Spanner. The default values are sufficient for
	// most application workloads.

	// MinSessions is the minimum number of Cloud Spanner sessions in the internal
	// session pool of PGAdapter. These sessions are created directly when PGAdapter
	// is started.
	MinSessions int
	// MaxSessions is the maximum number of Cloud Spanner sessions in the internal
	// session pool of PGAdapter. The maximum number of sessions should be set to
	// the maximum number of concurrent transactions that your application might
	// execute.
	MaxSessions int
	// NumChannels is the number of gRPC channels that PGAdapter creates and uses
	// to communicate with Cloud Spanner.
	NumChannels int
	// DatabaseRole specifies the fine-grained access control role that PGAdapter
	// should use when connecting to Cloud Spanner.
	DatabaseRole string

	// Port is the host TCP port where PGAdapter should listen for incoming connections.
	// The port must not be in use by any other process.
	// Use port 0 to dynamically assign an available port to PGAdapter.
	//
	// The port must be 0 (dynamic assignment) for execution environment Docker.
	Port int

	// Version is the PGAdapter version that should be started.
	// Leave this string empty to use the most recent version.
	Version string
}

func (config Config) toArguments() ([]string, error) {
	if config.CredentialsFile != "" && config.RequireAuthentication {
		return nil, fmt.Errorf("cannot set both a credentials file and require authentication")
	}
	if config.Instance != "" && config.Project == "" {
		return nil, fmt.Errorf("you must also set a project when you set an instance")
	}
	if config.Database != "" && config.Instance == "" {
		return nil, fmt.Errorf("you must also set an instance when you set a database")
	}
	if config.MinSessions < 0 {
		return nil, fmt.Errorf("MinSessions must be non-negative")
	}
	if config.MaxSessions < 0 {
		return nil, fmt.Errorf("MaxSessions must be non-negative")
	}
	if config.NumChannels < 0 {
		return nil, fmt.Errorf("NumChannels must be non-negative")
	}
	if config.MinSessions > config.MaxSessions {
		return nil, fmt.Errorf("MaxSessions must be >= MinSessions")
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
	if config.RequireAuthentication {
		cmd = append(cmd, "-a")
	}
	if config.CredentialsFile != "" {
		switch config.ExecutionEnvironment.(type) {
		case *Docker:
			cmd = append(cmd, "-c", "/credentials.json")
		case *Java:
			cmd = append(cmd, "-c", config.CredentialsFile)
		}
	}
	if config.MinSessions > 0 || config.MaxSessions > 0 || config.NumChannels > 0 || config.DatabaseRole != "" {
		jdbcOpts := ""
		if config.MinSessions > 0 {
			jdbcOpts += fmt.Sprintf("minSessions=%d;", config.MinSessions)
		}
		if config.MaxSessions > 0 {
			jdbcOpts += fmt.Sprintf("maxSessions=%d;", config.MaxSessions)
		}
		if config.NumChannels > 0 {
			jdbcOpts += fmt.Sprintf("numChannels=%d;", config.NumChannels)
		}
		if config.DatabaseRole != "" {
			jdbcOpts += fmt.Sprintf("databaseRole=%s;", config.DatabaseRole)
		}
		cmd = append(cmd, "-r", jdbcOpts)
	}
	return cmd, nil
}

type PGAdapter struct {
	container testcontainers.Container
	stopFunc  context.CancelFunc
	port      int
	stopped   bool
}

// Start starts a PGAdapter instance using the specified Config
// and returns a reference to a PGAdapter instance.
//
// The PGAdapter instance is started either as a Docker container or
// a Java application, depending on the ExecutionEnvironment that is
// set in the config. If no ExecutionEnvironment has been set, it
// will default to Java if that is available on this system, and
// otherwise fall back to Docker.
//
// The PGAdapter instance will automatically shut down when your
// application shuts down gracefully. It is not guaranteed that
// PGAdapter will be shut down if your application is killed or
// crashes. You can manually stop PGAdapter by calling Stop() on
// the returned instance.
//
// Call GetHostPort() to get the port where PGAdapter is listening for
// incoming connections.
func Start(ctx context.Context, config Config) (pgadapter *PGAdapter, err error) {
	autoDetect := config.ExecutionEnvironment == nil
	if config.ExecutionEnvironment == nil {
		if isJavaAvailable() {
			config.ExecutionEnvironment = &Java{}
		} else if isDockerAvailable() {
			config.ExecutionEnvironment = &Docker{}
		} else {
			return nil, fmt.Errorf("PGAdapter requires either Java or Docker to be installed on the local system")
		}
	}

	switch config.ExecutionEnvironment.(type) {
	case *Java:
		if !autoDetect && !isJavaAvailable() {
			return nil, fmt.Errorf("missing Java on local system")
		}
		return startJava(ctx, config)
	case *Docker:
		if !autoDetect && !isDockerAvailable() {
			return nil, fmt.Errorf("missing Docker on local system")
		}
		return startDocker(ctx, config)
	default:
		return nil, fmt.Errorf("unsupported or unknown execution environment: %v", config.ExecutionEnvironment)
	}
}

func isJavaAvailable() bool {
	cmd := exec.Command("java", "-version")
	if err := cmd.Start(); err != nil {
		return false
	}
	err := cmd.Wait()
	return err == nil
}

func isDockerAvailable() bool {
	cmd := exec.Command("docker", "--version")
	if err := cmd.Start(); err != nil {
		return false
	}
	err := cmd.Wait()
	return err == nil
}

func startJava(ctx context.Context, config Config) (*PGAdapter, error) {
	args, err := config.toArguments()
	if err != nil {
		return nil, err
	}
	dir, err := downloadAndUnpackJar(ctx, config)
	if err != nil {
		return nil, err
	}
	jar := filepath.Join(dir, "pgadapter.jar")
	var port int
	if config.Port == 0 {
		// Get a free TCP port.
		port, err = randomFreePort()
		if err != nil {
			return nil, err
		}
	} else {
		port = config.Port
	}
	args = append([]string{"-jar", jar}, args...)
	if config.Port == 0 {
		args = append(args, "-s", strconv.Itoa(port))
	}
	commandCtx, cancelFunc := context.WithCancel(context.Background())
	cmd := exec.CommandContext(commandCtx, "java", args...)
	cmd.Cancel = func() error {
		if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
	if err := cmd.Start(); err != nil {
		cancelFunc()
		return nil, err
	}
	// Wait for PGAdapter to start.
	if err := waitForPort(port, 50*time.Millisecond, 20); err != nil {
		cancelFunc()
		return nil, err
	}
	return &PGAdapter{stopFunc: cancelFunc, port: port}, nil
}

func waitForPort(port int, waitInterval time.Duration, maxAttempts int) error {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return err
	}
	for n := 0; n < maxAttempts; n++ {
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			time.Sleep(waitInterval)
			continue
		} else {
			_ = conn.Close()
			return nil
		}
	}
	return fmt.Errorf("failed to detect a running PGAdapter instance")
}

// randomFreePort returns a random free port that can then be assigned to PGAdapter.
func randomFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

const jarBaseDownloadUrl = "https://storage.googleapis.com/pgadapter-jar-releases/"
const latestJarFileName = "pgadapter.tar.gz"
const versionedJarFileName = "pgadapter-%s.tar.gz"

func downloadAndUnpackJar(ctx context.Context, config Config) (string, error) {
	ds := config.ExecutionEnvironment.(*Java).DownloadSettings
	var err error
	var dst string
	if ds.DownloadLocation == "" {
		dst, err = os.UserCacheDir()
		if err != nil {
			return "", err
		}
		dst = filepath.Join(dst, "pgadapter-downloads")
	} else {
		dst = ds.DownloadLocation
	}
	if !ds.DisableAutomaticDownload {
		if config.Version == "" {
			dst = filepath.Join(dst, "pgadapter-latest")
		} else {
			dst = filepath.Join(dst, fmt.Sprintf("pgadapter-%s", config.Version))
		}
		if err := os.MkdirAll(dst, 0755); err != nil {
			return "", err
		}
	}

	jarFi, jarErr := os.Stat(filepath.Join(dst, "pgadapter.jar"))
	libFi, libErr := os.Stat(filepath.Join(dst, "lib"))
	// If automatic downloads are disabled, then PGAdapter must already exist in the given location.
	if ds.DisableAutomaticDownload {
		if jarErr != nil {
			return "", jarErr
		}
		if libErr != nil {
			return "", libErr
		}
		// All seems OK, just return the download location.
		return ds.DownloadLocation, nil
	}

	var url string
	if config.Version == "" {
		url = jarBaseDownloadUrl + latestJarFileName
	} else {
		url = jarBaseDownloadUrl + fmt.Sprintf(versionedJarFileName, config.Version)
	}
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var lastModified time.Time
	lastModifiedString := resp.Header.Get("Last-Modified")
	if lastModifiedString != "" {
		lastModified, _ = http.ParseTime(lastModifiedString)
	}

	// Check if we need to download PGAdapter.
	if jarErr == nil && libErr == nil {
		// Both files seem to exist. Check the file info.
		if jarFi != nil && libFi != nil {
			// Check the types of the files and the last modified date.
			if libFi.IsDir() && !jarFi.IsDir() && libFi.ModTime().After(lastModified) && jarFi.ModTime().After(lastModified) {
				// Both the pgadapter.jar file and the lib dir exist and are up-to-date. Use these.
				return dst, nil
			}
		}
	}
	tarGzFile := filepath.Join(dst, "pgadapter.tar.gz")

	out, err := os.Create(tarGzFile)
	if err != nil {
		return "", err
	}
	defer out.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		return "", err
	}
	if err := unpackJar(ctx, tarGzFile); err != nil {
		return "", err
	}
	return dst, err
}

func unpackJar(ctx context.Context, tarGzFile string) error {
	dst := filepath.Dir(tarGzFile)
	reader, err := os.Open(tarGzFile)
	if err != nil {
		return err
	}
	gzr, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		switch {
		// if no more files are found return
		case err == io.EOF:
			return nil
		// return any other error
		case err != nil:
			return err
		case header == nil:
			continue
		}
		// Make sure that there are no relative paths in the gzipped file that could overwrite any existing files on
		// this system.
		if strings.Contains(header.Name, "..") {
			return fmt.Errorf("zipped file contains relative path: %s", header.Name)
		}
		target := filepath.Join(dst, header.Name)

		// check the file type
		switch header.Typeflag {
		// if it's a dir, and it doesn't exist, then create it
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					return err
				}
			}
		// if it's a file create it
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				return err
			}
			_ = f.Close()
		}
	}
}

func startDocker(ctx context.Context, config Config) (pgadapter *PGAdapter, err error) {
	if config.Port != 0 {
		return nil, fmt.Errorf("only dynamic port assignment (Config.Port=0) is supported for Docker")
	}
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
	cmd, err := config.toArguments()
	if err != nil {
		return nil, err
	}
	cmd = append(cmd, "-x")
	image := dockerImage
	if config.Version != "" {
		image += ":" + config.Version
	}
	req := testcontainers.ContainerRequest{
		AlwaysPullImage: config.ExecutionEnvironment.(*Docker).AlwaysPullImage,
		Image:           image,
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
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			if !config.ExecutionEnvironment.(*Docker).KeepContainer {
				hostConfig.AutoRemove = true
			}
		},
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

// GetHostPort returns the port on the current host machine where PGAdapter is listening
// for incoming PostgreSQL connections. Call this method to get the port number that was
// dynamically assigned to PGAdapter if you use dynamic port assignment.
func (pgadapter *PGAdapter) GetHostPort() (int, error) {
	if pgadapter == nil {
		return 0, fmt.Errorf("nil reference")
	}
	if pgadapter.stopped {
		return 0, fmt.Errorf("PGAdapter has been stopped")
	}
	if pgadapter.port == 0 {
		return 0, fmt.Errorf("PGAdapter has not been started successfully")
	}
	return pgadapter.port, nil
}

// Stop stops the PGAdapter instance.
func (pgadapter *PGAdapter) Stop(ctx context.Context) error {
	if pgadapter == nil {
		return fmt.Errorf("nil reference")
	}
	if pgadapter.stopped {
		return nil
	}
	defer func() {
		pgadapter.stopped = true
	}()
	if pgadapter.container != nil && pgadapter.container.IsRunning() {
		return pgadapter.container.Terminate(ctx)
	}
	if pgadapter.stopFunc != nil {
		pgadapter.stopFunc()
		pgadapter.stopFunc = nil
	}
	return nil
}
