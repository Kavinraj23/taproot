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

const selfCID = "a1d44c6143a878fe8baca396b0b5bf656677f202346f050287fc8fe79804d58c"

// A real excerpt of a container's /proc/self/mountinfo: Docker bind-mounts the container's
// hostname, hosts and resolv.conf from its own directory, named after its ID.
const containerMountinfo = `1203 1032 0:180 / / rw,relatime master:400 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/ABC
1212 1203 8:48 /docker/containers/` + selfCID + `/resolv.conf /etc/resolv.conf rw,relatime - ext4 /dev/sdd rw
1213 1203 8:48 /docker/containers/` + selfCID + `/hostname /etc/hostname rw,relatime - ext4 /dev/sdd rw
1214 1203 8:48 /docker/containers/` + selfCID + `/hosts /etc/hosts rw,relatime - ext4 /dev/sdd rw
`

const hostMountinfo = `22 1 8:2 / / rw,relatime shared:1 - ext4 /dev/sda2 rw
23 22 0:21 / /proc rw,nosuid,nodev,noexec,relatime shared:12 - proc proc rw
`

func TestSelfContainerID(t *testing.T) {
	if got := selfContainerID(containerMountinfo); got != selfCID {
		t.Errorf("in a container: %q, want %q", got, selfCID)
	}
	if got := selfContainerID(hostMountinfo); got != "" {
		t.Errorf("on a host: %q, want empty", got)
	}
}

// ownProjectFS has the agent itself (container selfCID) plus two postgres processes: one in
// Taproot's container cid ("deploy" project) and one in the demo's container cid2.
const cid2 = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func ownProjectFS() fstest.MapFS {
	fsys := pgFS() // pid 412 in container cid
	fsys["self/mountinfo"] = file(containerMountinfo)
	addProc(fsys, "900", "postgres", "postgres\x00", "", "net:[2]")
	fsys["900/cgroup"] = file("0::/docker/" + cid2 + "\n")
	fsys["900/net/tcp"] = file(tcpHeader + tcpRow("00000000:1538", "0A", "2001"))
	fsys["900/fd/3"] = link("socket:[2001]")
	return fsys
}

func containersWithSelf(selfProject string) []docker.Container {
	return []docker.Container{
		{ID: selfCID, Name: "deploy-agent-1", Project: selfProject},
		{ID: cid, Name: "deploy-postgres-1", Project: "deploy"},
		{ID: cid2, Name: "astronomy-db", Project: "opentelemetry-demo"},
	}
}

func containerNames(svcs []Service) []string {
	var out []string
	for _, s := range svcs {
		out = append(out, s.Container)
	}
	return out
}

func TestDiscoverSkipsOwnProject(t *testing.T) {
	list := func(context.Context) ([]docker.Container, error) { return containersWithSelf("deploy"), nil }
	svcs, _, _, err := Discover(context.Background(), ownProjectFS(), list)
	if err != nil {
		t.Fatal(err)
	}
	if got := containerNames(svcs); len(got) != 1 || got[0] != "astronomy-db" {
		t.Errorf("services in %v, want only astronomy-db (deploy-postgres-1 is Taproot's own)", got)
	}
}

func TestDiscoverKeepsAllWhenSelfUnknown(t *testing.T) {
	cases := map[string]func(context.Context) ([]docker.Container, error){
		"self not listed": func(context.Context) ([]docker.Container, error) { return containersWithSelf("deploy")[1:], nil },
		"self has no project": func(context.Context) ([]docker.Container, error) {
			return containersWithSelf(""), nil
		},
	}
	for name, list := range cases {
		svcs, _, _, err := Discover(context.Background(), ownProjectFS(), list)
		if err != nil {
			t.Fatal(err)
		}
		if len(svcs) != 2 {
			t.Errorf("%s: services in %v, want both postgres kept", name, containerNames(svcs))
		}
	}

	// Not in a container at all.
	fsys := ownProjectFS()
	fsys["self/mountinfo"] = file(hostMountinfo)
	svcs, _, _, _ := Discover(context.Background(), fsys,
		func(context.Context) ([]docker.Container, error) { return containersWithSelf("deploy"), nil })
	if len(svcs) != 2 {
		t.Errorf("on a host: services in %v, want both kept", containerNames(svcs))
	}
}

func TestDiscoverKeepsAllWithoutDocker(t *testing.T) {
	svcs, _, dockerErr, err := Discover(context.Background(), ownProjectFS(),
		func(context.Context) ([]docker.Container, error) { return nil, errors.New("no socket") })
	if err != nil || dockerErr == nil {
		t.Fatalf("err = %v, dockerErr = %v", err, dockerErr)
	}
	if len(svcs) != 2 {
		t.Errorf("got %d services, want both kept when Docker can't say which are ours", len(svcs))
	}
}

func TestOwnProject(t *testing.T) {
	if got := OwnProject(ownProjectFS(), containersWithSelf("deploy")); got != "deploy" {
		t.Errorf("OwnProject = %q, want deploy", got)
	}
}
