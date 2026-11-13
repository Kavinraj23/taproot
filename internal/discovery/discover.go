package discovery

import (
	"context"
	"io/fs"

	"taproot/internal/docker"
)

// ContainerLister returns the running containers (docker.Client.Containers in production).
type ContainerLister func(ctx context.Context) ([]docker.Container, error)

// Discover runs one full round: scan /proc, pick out the services, and name their containers.
// Docker is optional: if list fails, the services come back without container names and
// dockerErr says why. err is set only when /proc itself can't be read.
func Discover(ctx context.Context, fsys fs.FS, list ContainerLister) (svcs []Service, procs []Process, dockerErr, err error) {
	procs, err = Scan(fsys)
	if err != nil {
		return nil, nil, nil, err
	}
	svcs = Identify(procs)
	cs, dockerErr := list(ctx)
	if dockerErr == nil {
		svcs = Enrich(svcs, cs)
	}
	return svcs, procs, dockerErr, nil
}
