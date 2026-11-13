package discovery

import (
	"reflect"
	"testing"

	"taproot/internal/docker"
)

func TestEnrich(t *testing.T) {
	const otherID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	containers := []docker.Container{
		{ID: cid, Name: "astronomy-db", Image: "postgres:18.4", Project: "opentelemetry-demo", ComposeService: "postgresql"},
	}
	svcs := []Service{
		{Kind: "postgres", PID: 412, Name: "postgres", Port: 5432, ContainerID: cid},
		{Kind: "valkey", PID: 500, Name: "valkey-server", Port: 6379},             // host process
		{Kind: "kafka", PID: 300, Name: "java", Port: 9092, ContainerID: otherID}, // not in the list
	}
	orig := slicesClone(svcs)

	got := Enrich(svcs, containers)

	want := []Service{
		{Kind: "postgres", PID: 412, Name: "postgres", Port: 5432, ContainerID: cid,
			Container: "astronomy-db", Image: "postgres:18.4", Project: "opentelemetry-demo", ComposeService: "postgresql"},
		{Kind: "valkey", PID: 500, Name: "valkey-server", Port: 6379},
		{Kind: "kafka", PID: 300, Name: "java", Port: 9092, ContainerID: otherID},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Enrich =\n  %+v\nwant\n  %+v", got, want)
	}
	if !reflect.DeepEqual(svcs, orig) {
		t.Errorf("Enrich modified its input: %+v", svcs)
	}
}

func slicesClone(s []Service) []Service { return append([]Service(nil), s...) }
