package discovery

import "taproot/internal/docker"

// Enrich returns a copy of svcs with container names, images and compose labels filled in from
// cs, matched by container ID. Services with no container, or whose container isn't in cs (it
// started after the list was fetched), are returned as they are.
func Enrich(svcs []Service, cs []docker.Container) []Service {
	byID := make(map[string]docker.Container, len(cs))
	for _, c := range cs {
		byID[c.ID] = c
	}
	out := make([]Service, len(svcs))
	for i, s := range svcs {
		if c, ok := byID[s.ContainerID]; ok && s.ContainerID != "" {
			s.Container, s.Image, s.Project, s.ComposeService = c.Name, c.Image, c.Project, c.ComposeService
		}
		out[i] = s
	}
	return out
}
