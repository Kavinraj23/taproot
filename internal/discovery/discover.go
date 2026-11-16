package discovery

import (
	"context"
	"io/fs"

	"taproot/internal/docker"
)

// ContainerLister returns the running containers (docker.Client.Containers in production).
type ContainerLister func(ctx context.Context) ([]docker.Container, error)

// Discover runs one full round: scan /proc, pick out the services, name their containers, and
// leave out services in the agent's own compose project (Taproot doesn't measure itself).
// Docker is optional: if list fails, the services come back without container names, nothing is
// left out, and dockerErr says why. err is set only when /proc itself can't be read.
func Discover(ctx context.Context, fsys fs.FS, list ContainerLister) (svcs []Service, procs []Process, dockerErr, err error) {
	procs, err = Scan(fsys)
	if err != nil {
		return nil, nil, nil, err
	}
	svcs = Identify(procs)
	cs, dockerErr := list(ctx)
	if dockerErr == nil {
		svcs = Enrich(svcs, cs)
		if own := OwnProject(fsys, cs); own != "" {
			kept := svcs[:0:0]
			for _, s := range svcs {
				if s.Project != own {
					kept = append(kept, s)
				}
			}
			svcs = kept
		}
	}
	return svcs, procs, dockerErr, nil
}

// OwnProject returns the compose project of the container the agent runs in, found through
// fsys's self/mountinfo, or "" if the agent isn't in a listed compose container.
func OwnProject(fsys fs.FS, cs []docker.Container) string {
	b, err := fs.ReadFile(fsys, "self/mountinfo")
	if err != nil {
		return ""
	}
	id := selfContainerID(string(b))
	if id == "" {
		return ""
	}
	for _, c := range cs {
		if c.ID == id {
			return c.Project
		}
	}
	return ""
}
