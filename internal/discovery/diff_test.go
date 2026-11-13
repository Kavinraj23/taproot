package discovery

import (
	"reflect"
	"testing"
)

func pg(pid, port int) Service {
	return Service{Kind: "postgres", PID: pid, Name: "postgres", Port: port, Container: "astronomy-db"}
}
func vk(pid int) Service { return Service{Kind: "valkey", PID: pid, Name: "valkey-server", Port: 6379} }
func kf(pid int) Service { return Service{Kind: "kafka", PID: pid, Name: "java", Port: 9092} }

func TestDiff(t *testing.T) {
	tests := []struct {
		name      string
		prev, cur []Service
		want      Changes
	}{
		{name: "added", prev: nil, cur: []Service{pg(412, 5432)},
			want: Changes{Added: []Service{pg(412, 5432)}}},
		{name: "removed", prev: []Service{pg(412, 5432)}, cur: nil,
			want: Changes{Removed: []Service{pg(412, 5432)}}},
		{name: "port appeared", prev: []Service{pg(412, 0)}, cur: []Service{pg(412, 5432)},
			want: Changes{Updated: []Service{pg(412, 5432)}}},
		{name: "unchanged", prev: []Service{pg(412, 5432)}, cur: []Service{pg(412, 5432)},
			want: Changes{}},
		{name: "container restarted, new pid", prev: []Service{pg(412, 5432)}, cur: []Service{pg(999, 5432)},
			want: Changes{Added: []Service{pg(999, 5432)}, Removed: []Service{pg(412, 5432)}}},
		{
			name: "mixed",
			prev: []Service{pg(412, 5432), vk(500)},
			cur:  []Service{kf(300), vk(500)},
			want: Changes{Added: []Service{kf(300)}, Removed: []Service{pg(412, 5432)}},
		},
		{
			name: "results sorted by pid",
			prev: nil,
			cur:  []Service{vk(500), kf(300), pg(412, 5432)},
			want: Changes{Added: []Service{kf(300), pg(412, 5432), vk(500)}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Diff(tt.prev, tt.cur)
			if !sameChanges(got, tt.want) {
				t.Errorf("Diff =\n  %+v\nwant\n  %+v", got, tt.want)
			}
			if got.Empty() != (len(tt.want.Added)+len(tt.want.Removed)+len(tt.want.Updated) == 0) {
				t.Errorf("Empty() = %v for %+v", got.Empty(), got)
			}
		})
	}
}

// sameChanges treats nil and empty lists as equal.
func sameChanges(a, b Changes) bool {
	eq := func(x, y []Service) bool { return len(x) == 0 && len(y) == 0 || reflect.DeepEqual(x, y) }
	return eq(a.Added, b.Added) && eq(a.Removed, b.Removed) && eq(a.Updated, b.Updated)
}
