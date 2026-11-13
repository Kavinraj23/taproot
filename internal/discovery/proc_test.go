package discovery

import (
	"io/fs"
	"reflect"
	"testing"
	"testing/fstest"
)

const tcpHeader = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"

// tcpRow builds one /proc/net/tcp-format row; only local address, state and inode matter here.
func tcpRow(local, state, inode string) string {
	return "   0: " + local + " 00000000:0000 " + state + " 00000000:00000000 00:00000000 00000000   999        0 " + inode + " 1 0000000000000000 100 0 0 10 0\n"
}

func TestParseNetTCP(t *testing.T) {
	tests := []struct {
		name string
		text string
		want map[uint64]int
	}{
		{
			name: "listen row kept, established ignored",
			text: tcpHeader + tcpRow("00000000:1538", "0A", "1001") + tcpRow("0100007F:1538", "01", "1002"),
			want: map[uint64]int{1001: 5432},
		},
		{
			name: "tcp6 format",
			text: tcpHeader + tcpRow("00000000000000000000000000000000:18EB", "0A", "2002"),
			want: map[uint64]int{2002: 6379},
		},
		{name: "header only", text: tcpHeader, want: map[uint64]int{}},
		{name: "empty", text: "", want: map[uint64]int{}},
		{
			name: "malformed rows skipped",
			text: tcpHeader +
				"   0: 00000000:1538 00000000:0000 0A\n" + // too few fields
				tcpRow("00000000:ZZZZ", "0A", "3001") + // non-hex port
				tcpRow("00000000:1F90", "0A", "3002"),
			want: map[uint64]int{3002: 8080},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseNetTCP(tt.text)
			if len(got) != 0 || len(tt.want) != 0 {
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("parseNetTCP = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestSocketInode(t *testing.T) {
	tests := []struct {
		target string
		want   uint64
		wantOK bool
	}{
		{"socket:[12345]", 12345, true},
		{"pipe:[9]", 0, false},
		{"/dev/null", 0, false},
		{"socket:[abc]", 0, false},
	}
	for _, tt := range tests {
		got, ok := socketInode(tt.target)
		if ok != tt.wantOK || (ok && got != tt.want) {
			t.Errorf("socketInode(%q) = %d, %v; want %d, %v", tt.target, got, ok, tt.want, tt.wantOK)
		}
	}
}

func TestParseCmdline(t *testing.T) {
	if got, want := parseCmdline([]byte("postgres\x00-c\x00x=1\x00")), []string{"postgres", "-c", "x=1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("parseCmdline = %q, want %q", got, want)
	}
	if got := parseCmdline(nil); len(got) != 0 {
		t.Errorf("parseCmdline(empty) = %q, want empty", got)
	}
}

func TestParseStatPPID(t *testing.T) {
	tests := []struct {
		text   string
		want   int
		wantOK bool
	}{
		{"412 (postgres) S 1 412 412 0 -1 4194560 ...\n", 1, true},
		{"77 (my (weird) prog) S 9 77 77 0 -1 ...\n", 9, true},
		{"", 0, false},
		{"412 postgres S 1 412", 0, false}, // no parens
		{"412 (postgres)", 0, false},       // nothing after comm
		{"412 (postgres) S x 412", 0, false},
	}
	for _, tt := range tests {
		got, ok := parseStatPPID(tt.text)
		if got != tt.want || ok != tt.wantOK {
			t.Errorf("parseStatPPID(%q) = %d, %v; want %d, %v", tt.text, got, ok, tt.want, tt.wantOK)
		}
	}
}

func TestScanReadsPPID(t *testing.T) {
	fsys := fstest.MapFS{}
	addProc(fsys, "420", "postgres", "postgres: checkpointer \x00", "", "")
	fsys["420/stat"] = file("420 (postgres) S 412 412 412 0 -1 ...\n")
	addProc(fsys, "421", "postgres", "postgres: walwriter \x00", "", "") // no stat file

	got := scanByPID(t, fsys)
	if got[420].PPID != 412 {
		t.Errorf("pid 420 PPID = %d, want 412", got[420].PPID)
	}
	if p, ok := got[421]; !ok || p.PPID != 0 {
		t.Errorf("pid 421 = %+v, %v; want kept with PPID 0", p, ok)
	}
}

func link(target string) *fstest.MapFile {
	return &fstest.MapFile{Mode: fs.ModeSymlink, Data: []byte(target)}
}

func file(s string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(s)} }

// addProc adds the common files of one process to fsys.
func addProc(fsys fstest.MapFS, pid, comm, cmdline, exe, netns string) {
	fsys[pid+"/comm"] = file(comm + "\n")
	fsys[pid+"/cmdline"] = file(cmdline)
	if exe != "" {
		fsys[pid+"/exe"] = link(exe)
	}
	if netns != "" {
		fsys[pid+"/ns/net"] = link(netns)
	}
}

func scanByPID(t *testing.T, fsys fs.FS) map[int]Process {
	t.Helper()
	procs, err := Scan(fsys)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	byPID := map[int]Process{}
	for i, p := range procs {
		if i > 0 && procs[i-1].PID >= p.PID {
			t.Errorf("result not sorted by PID: %d before %d", procs[i-1].PID, p.PID)
		}
		byPID[p.PID] = p
	}
	return byPID
}

func TestScanBasic(t *testing.T) {
	const ns = "net:[4026531840]"
	tcp := tcpHeader + tcpRow("00000000:1538", "0A", "1001") + tcpRow("0100007F:1538", "01", "1003")
	fsys := fstest.MapFS{
		// Non-pid entries at the root.
		"self/comm": file("agent\n"),
		"net/tcp":   file(tcpHeader + tcpRow("00000000:0050", "0A", "1001")), // must not be used
		"cpuinfo":   file("processor: 0\n"),
	}

	// pid 10: listens on 5432, plus a non-socket fd and an established socket.
	addProc(fsys, "10", "postgres", "postgres\x00-D\x00/data\x00", "/usr/lib/postgresql/16/bin/postgres", ns)
	fsys["10/net/tcp"] = file(tcp)
	fsys["10/net/tcp6"] = file(tcpHeader)
	fsys["10/fd/0"] = link("/dev/null")
	fsys["10/fd/3"] = link("socket:[1001]")
	fsys["10/fd/4"] = link("socket:[1003]") // established: not a listen port

	// pid 20: same netns, no sockets.
	addProc(fsys, "20", "sleep", "sleep\x00100\x00", "/bin/sleep", ns)
	fsys["20/net/tcp"] = file(tcp)
	fsys["20/fd/0"] = link("/dev/null")

	// pid 2: kernel thread.
	addProc(fsys, "2", "kthreadd", "", "", "")

	// pid 30: exited mid-scan (comm still there, cmdline gone).
	fsys["30/comm"] = file("gone\n")

	// pid 40: exe and fd unreadable.
	addProc(fsys, "40", "secret", "secret\x00", "", "")

	got := scanByPID(t, fsys)

	if _, ok := got[2]; ok {
		t.Error("kernel thread pid 2 should be skipped")
	}
	if _, ok := got[30]; ok {
		t.Error("exited pid 30 should be skipped")
	}
	if len(got) != 3 {
		t.Errorf("got %d processes, want 3 (10, 20, 40): %+v", len(got), got)
	}

	want10 := Process{
		PID:         10,
		Name:        "postgres",
		Exe:         "/usr/lib/postgresql/16/bin/postgres",
		Cmdline:     []string{"postgres", "-D", "/data"},
		ListenPorts: []int{5432},
	}
	if !reflect.DeepEqual(got[10], want10) {
		t.Errorf("pid 10 = %+v, want %+v", got[10], want10)
	}
	if p := got[20]; p.Name != "sleep" || len(p.ListenPorts) != 0 {
		t.Errorf("pid 20 = %+v, want name sleep and no ports", p)
	}
	if p := got[40]; p.Name != "secret" || p.Exe != "" || len(p.ListenPorts) != 0 {
		t.Errorf("pid 40 = %+v, want name secret, empty exe, no ports", p)
	}
}

func TestScanSeparateNetns(t *testing.T) {
	fsys := fstest.MapFS{}
	addProc(fsys, "50", "java", "java\x00kafka.Kafka\x00", "/usr/bin/java", "net:[100]")
	fsys["50/net/tcp"] = file(tcpHeader + tcpRow("00000000:2384", "0A", "7"))
	fsys["50/fd/3"] = link("socket:[7]")

	addProc(fsys, "60", "valkey-server", "valkey-server *:6379\x00", "/usr/bin/valkey-server", "net:[200]")
	fsys["60/net/tcp"] = file(tcpHeader + tcpRow("00000000:18EB", "0A", "7"))
	fsys["60/fd/3"] = link("socket:[7]")

	got := scanByPID(t, fsys)
	if !reflect.DeepEqual(got[50].ListenPorts, []int{9092}) {
		t.Errorf("pid 50 ports = %v, want [9092]", got[50].ListenPorts)
	}
	if !reflect.DeepEqual(got[60].ListenPorts, []int{6379}) {
		t.Errorf("pid 60 ports = %v, want [6379]", got[60].ListenPorts)
	}
}

func TestScanDedupesPortsAcrossV4AndV6(t *testing.T) {
	fsys := fstest.MapFS{}
	addProc(fsys, "70", "redis", "redis\x00", "/usr/bin/redis", "net:[300]")
	fsys["70/net/tcp"] = file(tcpHeader + tcpRow("00000000:18EB", "0A", "11"))
	fsys["70/net/tcp6"] = file(tcpHeader + tcpRow("00000000000000000000000000000000:18EB", "0A", "12"))
	fsys["70/fd/3"] = link("socket:[11]")
	fsys["70/fd/4"] = link("socket:[12]")

	got := scanByPID(t, fsys)
	if !reflect.DeepEqual(got[70].ListenPorts, []int{6379}) {
		t.Errorf("pid 70 ports = %v, want [6379]", got[70].ListenPorts)
	}
}
