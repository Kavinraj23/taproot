// Package docker is a minimal client for the one Docker Engine API call discovery needs: listing
// running containers with their names, images and compose labels.
package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Container is a running container as discovery needs it.
type Container struct {
	ID             string // full 64-hex ID, the same one that appears in /proc/<pid>/cgroup
	Name           string // e.g. "astronomy-db"
	Image          string // e.g. "postgres:18.4"
	Project        string // compose project, "" if not started by compose
	ComposeService string // compose service name, "" if not started by compose
}

// Client talks to the Docker Engine API at BaseURL.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// requestTimeout bounds each call, so a stuck daemon can't stall discovery.
const requestTimeout = 5 * time.Second

// NewUnixClient returns a client for the daemon's unix socket (usually /var/run/docker.sock).
// The host in BaseURL is a placeholder: every request is dialled to the socket.
func NewUnixClient(socketPath string) *Client {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socketPath)
		},
	}
	return &Client{BaseURL: "http://docker", HTTP: &http.Client{Transport: tr}}
}

type apiContainer struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Image  string            `json:"Image"`
	Labels map[string]string `json:"Labels"`
}

// Containers lists the running containers.
func (c *Client) Containers(ctx context.Context) ([]Container, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/containers/json", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("docker: GET /containers/json: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var raw []apiContainer
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("docker: decode containers: %w", err)
	}
	out := make([]Container, 0, len(raw))
	for _, a := range raw {
		c := Container{
			ID:             a.ID,
			Image:          a.Image,
			Project:        a.Labels["com.docker.compose.project"],
			ComposeService: a.Labels["com.docker.compose.service"],
		}
		if len(a.Names) > 0 {
			c.Name = strings.TrimPrefix(a.Names[0], "/")
		}
		out = append(out, c)
	}
	return out, nil
}
