package products

import (
	"context"
	"fmt"
	"net"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultUser = "test"
	defaultPort = "8080/tcp"
)

// TrinoContainer represents the Trino container type used in the module
type TrinoContainer struct {
	testcontainers.Container
	user string
}

// // related to configurable options

// type Option func(*options)

// MustConnectionString panics if the address cannot be determined.
func (c *TrinoContainer) MustConnectionString(ctx context.Context, args ...string) string {
	addr, err := c.ConnectionString(ctx, args...)
	if err != nil {
		panic(err)
	}
	return addr
}

// ConnectionString returns the connection string for the Trino container
func (c *TrinoContainer) ConnectionString(ctx context.Context, args ...string) (string, error) {
	containerPort, err := c.MappedPort(ctx, defaultPort)
	if err != nil {
		return "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}

	connStr := fmt.Sprintf("http://%s@%s/", c.user, net.JoinHostPort(host, containerPort.Port()))
	fmt.Println(connStr)

	return connStr, nil
}

// Run creates an instance of the Trino container type
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*TrinoContainer, error) {
	req := testcontainers.ContainerRequest{
		Image: img,
		Env: map[string]string{
			"USER": defaultUser,
		},
		ExposedPorts: []string{
			defaultPort,
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("======== SERVER STARTED ========"),
			wait.ForExposedPort(),
			wait.ForHTTP("http://:8080/"),
			// TODO: issue a select query until it returns a result
		),
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	// Gather all config options (defaults and then apply provided options)
	for _, opt := range opts {
		if err := opt.Customize(&genericContainerReq); err != nil {
			return nil, err
		}
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	var c *TrinoContainer
	if container != nil {
		c = &TrinoContainer{
			Container: container,
			user:      req.Env["USER"],
		}
	}

	if err != nil {
		return c, fmt.Errorf("generic container: %w", err)
	}

	return c, nil
}

// WithCmd replace the run command with the given command and options
func WithCmd(cmd string, options ...string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append([]string{cmd}, options...)
		return nil
	}
}
