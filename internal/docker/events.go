package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Event is a container starting or stopping.
type Event struct {
	Action      string // "start" or "die"
	ContainerID string
}

// eventFilters asks the daemon for container start and die events only.
var eventFilters = func() string {
	b, _ := json.Marshal(map[string][]string{"type": {"container"}, "event": {"start", "die"}})
	return string(b)
}()

type apiEvent struct {
	Action string `json:"Action"`
	Actor  struct {
		ID string `json:"ID"`
	} `json:"Actor"`
}

// Events streams container start and die events to fn until ctx ends or the stream breaks. It
// always returns an error: ctx's when cancelled, otherwise why the stream ended. Unlike
// Containers it has no timeout, since the stream is meant to stay open.
func (c *Client) Events(ctx context.Context, fn func(Event)) error {
	u := c.BaseURL + "/events?filters=" + url.QueryEscape(eventFilters)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("docker: GET /events: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	dec := json.NewDecoder(resp.Body)
	for {
		var e apiEvent
		if err := dec.Decode(&e); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return errors.New("docker: event stream ended")
			}
			return fmt.Errorf("docker: decode event: %w", err)
		}
		fn(Event{Action: e.Action, ContainerID: e.Actor.ID})
	}
}
