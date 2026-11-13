// Package discovery finds what is running on a host. Scan reads /proc for processes and the TCP
// ports they listen on; later layers decide what each process is.
package discovery

import (
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
)

// Process is one user-space process seen in /proc.
type Process struct {
	PID         int      `json:"pid"`
	PPID        int      `json:"ppid"`         // parent PID from /proc/<pid>/stat; 0 if unreadable
	Name       string   `json:"name"`         // /proc/<pid>/comm
	Exe         string   `json:"exe"`          // target of /proc/<pid>/exe; "" if unreadable
	Cmdline     []string `json:"cmdline"`      // /proc/<pid>/cmdline split on NUL
	ListenPorts []int    `json:"listen_ports"` // TCP ports in LISTEN state (v4 and v6), sorted, deduped
}

// tcpListen is the state column value for LISTEN in /proc/net/tcp.
const tcpListen = "0A"

// Scan reads every process in fsys, a filesystem rooted at /proc (os.DirFS("/proc") in
// production). Kernel threads and processes that exit during the scan are left out. Unreadable
// details (exe, fds) leave fields empty rather than failing. The result is sorted by PID.
func Scan(fsys fs.FS) ([]Process, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	// Listening sockets belong to a network namespace, and each container has its own, so the
	// tables are read per process and cached by namespace.
	listenByNetns := map[string]map[uint64]int{}

	var procs []Process
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 0 {
			continue // self, net, cpuinfo, ...
		}
		p, ok := readProcess(fsys, e.Name(), pid, listenByNetns)
		if ok {
			procs = append(procs, p)
		}
	}
	slices.SortFunc(procs, func(a, b Process) int { return a.PID - b.PID })
	return procs, nil
}

// readProcess returns ok false for kernel threads and processes that are gone.
func readProcess(fsys fs.FS, dir string, pid int, listenByNetns map[string]map[uint64]int) (Process, bool) {
	comm, err := fs.ReadFile(fsys, path.Join(dir, "comm"))
	if err != nil {
		return Process{}, false
	}
	rawCmd, err := fs.ReadFile(fsys, path.Join(dir, "cmdline"))
	if err != nil {
		return Process{}, false
	}
	cmdline := parseCmdline(rawCmd)
	if len(cmdline) == 0 {
		return Process{}, false // kernel thread
	}

	p := Process{
		PID:         pid,
		Name:        strings.TrimSuffix(string(comm), "\n"),
		Cmdline:     cmdline,
		ListenPorts: []int{},
	}
	if stat, err := fs.ReadFile(fsys, path.Join(dir, "stat")); err == nil {
		p.PPID, _ = parseStatPPID(string(stat))
	}
	if exe, err := fs.ReadLink(fsys, path.Join(dir, "exe")); err == nil {
		p.Exe = exe
	}
	p.ListenPorts = listenPorts(fsys, dir, listenTable(fsys, dir, listenByNetns))
	return p, true
}

// listenTable returns inode -> port for the LISTEN sockets in the process's network namespace.
func listenTable(fsys fs.FS, dir string, cache map[string]map[uint64]int) map[uint64]int {
	ns, nsErr := fs.ReadLink(fsys, path.Join(dir, "ns", "net"))
	if nsErr == nil {
		if t, ok := cache[ns]; ok {
			return t
		}
	}
	table := map[uint64]int{}
	for _, name := range []string{"tcp", "tcp6"} {
		b, err := fs.ReadFile(fsys, path.Join(dir, "net", name))
		if err != nil {
			continue
		}
		for inode, port := range parseNetTCP(string(b)) {
			table[inode] = port
		}
	}
	if nsErr == nil {
		cache[ns] = table
	}
	return table
}

// listenPorts matches the process's socket fds against the namespace's LISTEN table.
func listenPorts(fsys fs.FS, dir string, table map[uint64]int) []int {
	ports := []int{}
	if len(table) == 0 {
		return ports
	}
	fdDir := path.Join(dir, "fd")
	fds, err := fs.ReadDir(fsys, fdDir)
	if err != nil {
		return ports // no permission, or the process is gone
	}
	for _, fd := range fds {
		target, err := fs.ReadLink(fsys, path.Join(fdDir, fd.Name()))
		if err != nil {
			continue
		}
		inode, ok := socketInode(target)
		if !ok {
			continue
		}
		if port, ok := table[inode]; ok && !slices.Contains(ports, port) {
			ports = append(ports, port)
		}
	}
	slices.Sort(ports)
	return ports
}

// parseNetTCP reads /proc/net/tcp or tcp6 text and returns inode -> local port for LISTEN rows.
// Malformed rows are skipped.
func parseNetTCP(text string) map[uint64]int {
	out := map[uint64]int{}
	lines := strings.Split(text, "\n")
	for _, line := range lines[min(1, len(lines)):] { // first line is the header
		f := strings.Fields(line)
		// sl local rem st tx:rx tr:when retrnsmt uid timeout inode
		if len(f) < 10 || f[3] != tcpListen {
			continue
		}
		i := strings.LastIndexByte(f[1], ':')
		if i < 0 {
			continue
		}
		port, err := strconv.ParseUint(f[1][i+1:], 16, 16)
		if err != nil {
			continue
		}
		inode, err := strconv.ParseUint(f[9], 10, 64)
		if err != nil || inode == 0 {
			continue
		}
		out[inode] = int(port)
	}
	return out
}

// socketInode extracts the inode from an fd link target like "socket:[12345]".
func socketInode(target string) (uint64, bool) {
	s, ok := strings.CutPrefix(target, "socket:[")
	if !ok {
		return 0, false
	}
	s, ok = strings.CutSuffix(s, "]")
	if !ok {
		return 0, false
	}
	inode, err := strconv.ParseUint(s, 10, 64)
	return inode, err == nil
}

// parseStatPPID returns the parent PID from /proc/<pid>/stat text: "pid (comm) state ppid ...".
// comm may contain spaces and parentheses, so the fields are read after the last ')'.
func parseStatPPID(text string) (int, bool) {
	i := strings.LastIndexByte(text, ')')
	if i < 0 {
		return 0, false
	}
	f := strings.Fields(text[i+1:])
	if len(f) < 2 {
		return 0, false
	}
	ppid, err := strconv.Atoi(f[1])
	if err != nil {
		return 0, false
	}
	return ppid, true
}

// parseCmdline splits /proc/<pid>/cmdline on NUL. Empty input (a kernel thread) gives nil.
func parseCmdline(b []byte) []string {
	s := strings.TrimRight(string(b), "\x00")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\x00")
}
