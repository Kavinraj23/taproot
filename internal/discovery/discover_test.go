package discovery

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	"taproot/internal/docker"
)

func pgFS() fstest.MapFS {
	fsys := fstest.MapFS{}
	addProc(fsys, "412", "postgres", "postgres\x00", "", "net:[1]")
	fsys["412/cgroup"] = file("0::/docker/" + cid + "\n")
	fsys["412/net/tcp"] = file(tcpHeader + tcpRow("00000000:1538", "0A", "1001"))
	fsys["412/fd/3"] = link("socket:[1001]")
	return fsys
}

func TestDiscoverEnriches(t *testing.T) {
	list := func(context.Context) ([]docker.Container, error) {
		return []docker.Container{{ID: cid, Name: "astronomy-db", Image: "postgres:18.4"}}, nil
	}
	svcs, procs, dockerErr, err := Discover(context.Background(), pgFS(), list)
	if err != nil || dockerErr != nil {
		t.Fatalf("Discover err = %v, dockerErr = %v", err, dockerErr)
	}
	if len(procs) != 1 {
		t.Errorf("procs = %+v, want 1", procs)
	}
	if len(svcs) != 1 || svcs[0].Container != "astronomy-db" || svcs[0].Port != 5432 {
		t.Errorf("svcs = %+v, want postgres in astronomy-db on 5432", svcs)
	}
}

func TestDiscoverWithoutDocker(t *testing.T) {
	list := func(context.Context) ([]docker.Container, error) { return nil, errors.New("no socket") }
	svcs, _, dockerErr, err := Discover(context.Background(), pgFS(), list)
	if err != nil {
		t.Fatalf("Discover err = %v, want nil (docker is optional)", err)
	}
	if dockerErr == nil {
		t.Error("dockerErr = nil, want the lister's error")
	}
	if len(svcs) != 1 || svcs[0].Container != "" || svcs[0].ContainerID != cid {
		t.Errorf("svcs = %+v, want postgres with container ID but no name", svcs)
	}
}
