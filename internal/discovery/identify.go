package discovery

import (
	"path"
	"slices"
	"strings"
)

// Service is one recognised instance of something Taproot has a sensor for.
type Service struct {
	Kind string `json:"kind"` // "postgres", "valkey" or "kafka"
	PID  int    `json:"pid"`  // the instance's root process, e.g. the Postgres postmaster
	Name string `json:"name"` // process name, for display
	Port int    `json:"port"` // listening port; 0 if unknown

	// Container details. ContainerID comes from /proc; the rest from Docker (see Enrich).
	ContainerID    string `json:"container_id,omitempty"`
	Container      string `json:"container,omitempty"`
	Image          string `json:"image,omitempty"`
	Project        string `json:"project,omitempty"`
	ComposeService string `json:"compose_service,omitempty"`
}

// rule recognises one kind of service by what the process is. Ports never decide the kind
// (Redpanda listens on Kafka's 9092); they only pick which port to record.
type rule struct {
	kind        string
	names       []string // matched against the process name and the exe's base name
	mainArg     string   // if set, the cmdline must also contain this exact argument
	defaultPort int
}

var rules = []rule{
	{kind: "postgres", names: []string{"postgres"}, defaultPort: 5432},
	{kind: "valkey", names: []string{"valkey-server", "redis-server"}, defaultPort: 6379},
	// Kafka is a JVM like many other services, so its main class tells it apart.
	{kind: "kafka", names: []string{"java"}, mainArg: "kafka.Kafka", defaultPort: 9092},
}

// Identify picks out the known services in a process list, one per instance, sorted by PID.
// A matched process whose parent is the same kind (Postgres's per-connection and background
// processes) is part of its parent's instance, not a service of its own.
func Identify(procs []Process) []Service {
	kinds := make(map[int]string, len(procs))
	for _, p := range procs {
		if r, ok := match(p); ok {
			kinds[p.PID] = r.kind
		}
	}

	var out []Service
	for _, p := range procs {
		r, ok := match(p)
		if !ok || kinds[p.PPID] == r.kind {
			continue
		}
		out = append(out, Service{
			Kind:        r.kind,
			PID:         p.PID,
			Name:        p.Name,
			Port:        pickPort(p.ListenPorts, r.defaultPort),
			ContainerID: p.ContainerID,
		})
	}
	slices.SortFunc(out, func(a, b Service) int { return a.PID - b.PID })
	return out
}

// kindOf returns the kind of service p is, or "" if it isn't one Taproot knows.
func kindOf(p Process) string {
	r, _ := match(p)
	return r.kind
}

func match(p Process) (rule, bool) {
	exe := path.Base(strings.TrimSuffix(p.Exe, " (deleted)"))
	for _, r := range rules {
		if !slices.Contains(r.names, p.Name) && (p.Exe == "" || !slices.Contains(r.names, exe)) {
			continue
		}
		if r.mainArg != "" && !slices.Contains(p.Cmdline, r.mainArg) {
			continue
		}
		return r, true
	}
	return rule{}, false
}

// pickPort prefers the kind's default port, then the lowest listening port, then 0.
func pickPort(ports []int, defaultPort int) int {
	if slices.Contains(ports, defaultPort) {
		return defaultPort
	}
	if len(ports) == 0 {
		return 0
	}
	return slices.Min(ports)
}
