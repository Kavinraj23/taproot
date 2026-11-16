package discovery

import (
	"reflect"
	"testing"
)

func TestKindOf(t *testing.T) {
	tests := []struct {
		name string
		p    Process
		want string
	}{
		{
			name: "postgres postmaster",
			p: Process{Name: "postgres", Exe: "/usr/lib/postgresql/18/bin/postgres",
				Cmdline: []string{"postgres", "-c", "shared_preload_libraries=pg_stat_statements"}},
			want: "postgres",
		},
		{
			name: "valkey",
			p:    Process{Name: "valkey-server", Cmdline: []string{"valkey-server *:6379"}},
			want: "valkey",
		},
		{
			name: "redis counts as valkey",
			p:    Process{Name: "redis-server", Cmdline: []string{"redis-server *:6379"}},
			want: "valkey",
		},
		{
			name: "kafka jvm",
			p: Process{Name: "java", Exe: "/opt/java/openjdk/bin/java",
				Cmdline: []string{"java", "-Xmx1G", "-cp", "/opt/kafka/libs/*", "kafka.Kafka", "/opt/kafka/config/server.properties"}},
			want: "kafka",
		},
		{
			name: "other java app is not kafka",
			p: Process{Name: "java", Exe: "/opt/java/openjdk/bin/java",
				Cmdline: []string{"java", "-jar", "/usr/src/app/opentelemetry-demo-ad.jar"}, ListenPorts: []int{9555}},
			want: "",
		},
		{
			name: "redpanda on 9092 is not kafka",
			p:    Process{Name: "redpanda", Exe: "/opt/redpanda/libexec/redpanda", Cmdline: []string{"redpanda", "start"}, ListenPorts: []int{9092}},
			want: "",
		},
		{
			name: "unknown name on 5432 is not postgres",
			p:    Process{Name: "pgproxy", Cmdline: []string{"pgproxy"}, ListenPorts: []int{5432}},
			want: "",
		},
		{
			name: "postgres with unreadable exe",
			p:    Process{Name: "postgres", Cmdline: []string{"postgres"}},
			want: "postgres",
		},
		{
			name: "deleted exe still matches",
			p:    Process{Name: "pg-renamed", Exe: "/usr/lib/postgresql/18/bin/postgres (deleted)", Cmdline: []string{"pg-renamed"}},
			want: "postgres",
		},
		{
			name: "kafka.Kafka inside a longer arg does not count",
			p:    Process{Name: "java", Cmdline: []string{"java", "-Dfoo=kafka.Kafka", "-jar", "app.jar"}},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := kindOf(tt.p); got != tt.want {
				t.Errorf("kindOf = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIdentifyCarriesContainerID(t *testing.T) {
	p := pgMaster(412, 5432)
	p.ContainerID = cid
	got := Identify([]Process{p})
	if len(got) != 1 || got[0].ContainerID != cid {
		t.Errorf("Identify = %+v, want one service with ContainerID %q", got, cid)
	}
}

func pgMaster(pid int, ports ...int) Process {
	return Process{PID: pid, PPID: 1, Name: "postgres", Cmdline: []string{"postgres"}, ListenPorts: ports}
}

func pgChild(pid, ppid int, role string) Process {
	return Process{PID: pid, PPID: ppid, Name: "postgres", Cmdline: []string{"postgres: " + role + " "}, ListenPorts: []int{}}
}

func kafkaProc(pid int, ports ...int) Process {
	return Process{PID: pid, PPID: 1, Name: "java", Cmdline: []string{"java", "kafka.Kafka", "server.properties"}, ListenPorts: ports}
}

func valkeyProc(pid int, ports ...int) Process {
	return Process{PID: pid, PPID: 1, Name: "valkey-server", Cmdline: []string{"valkey-server *:6379"}, ListenPorts: ports}
}

func TestIdentify(t *testing.T) {
	tests := []struct {
		name  string
		procs []Process
		want  []Service
	}{
		{
			name:  "postgres children fold into the postmaster",
			procs: []Process{pgMaster(412, 5432), pgChild(420, 412, "checkpointer"), pgChild(421, 412, "walwriter")},
			want:  []Service{{Kind: "postgres", PID: 412, Name: "postgres", Port: 5432}},
		},
		{
			name:  "two separate postgres instances",
			procs: []Process{pgMaster(412, 5432), pgChild(420, 412, "checkpointer"), pgMaster(900, 5432)},
			want: []Service{
				{Kind: "postgres", PID: 412, Name: "postgres", Port: 5432},
				{Kind: "postgres", PID: 900, Name: "postgres", Port: 5432},
			},
		},
		{
			name:  "default port preferred",
			procs: []Process{kafkaProc(300, 9092, 9093)},
			want:  []Service{{Kind: "kafka", PID: 300, Name: "java", Port: 9092}},
		},
		{
			name:  "lowest port when default absent",
			procs: []Process{pgMaster(412, 7000, 6543)},
			want:  []Service{{Kind: "postgres", PID: 412, Name: "postgres", Port: 6543}},
		},
		{
			name:  "no ports still a service",
			procs: []Process{valkeyProc(500)},
			want:  []Service{{Kind: "valkey", PID: 500, Name: "valkey-server", Port: 0}},
		},
		{
			name: "mixed host",
			procs: []Process{
				{PID: 1, Name: "tini", Cmdline: []string{"tini"}},
				kafkaProc(300, 9092, 9093),
				pgMaster(412, 5432), pgChild(420, 412, "checkpointer"),
				valkeyProc(500, 6379),
				{PID: 600, PPID: 1, Name: "java", Cmdline: []string{"java", "-jar", "ad.jar"}, ListenPorts: []int{9555}},
				{PID: 700, PPID: 1, Name: "redpanda", Cmdline: []string{"redpanda", "start"}, ListenPorts: []int{9092}},
				{PID: 800, PPID: 1, Name: "sleep", Cmdline: []string{"sleep", "100"}},
			},
			want: []Service{
				{Kind: "kafka", PID: 300, Name: "java", Port: 9092},
				{Kind: "postgres", PID: 412, Name: "postgres", Port: 5432},
				{Kind: "valkey", PID: 500, Name: "valkey-server", Port: 6379},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Identify(tt.procs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Identify =\n  %+v\nwant\n  %+v", got, tt.want)
			}
		})
	}
}

func TestSettled(t *testing.T) {
	cases := []struct {
		s    Service
		want bool
	}{
		{Service{Kind: "kafka", Port: 9092}, true},
		{Service{Kind: "kafka", Port: 9093}, false}, // controller port open, client port not yet
		{Service{Kind: "kafka", Port: 0}, false},
		{Service{Kind: "postgres", Port: 5432}, true},
		{Service{Kind: "unknown", Port: 1234}, true}, // no usual port to wait for
	}
	for _, c := range cases {
		if got := Settled(c.s); got != c.want {
			t.Errorf("Settled(%s on %d) = %v, want %v", c.s.Kind, c.s.Port, got, c.want)
		}
	}
}
