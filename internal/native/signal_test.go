package native

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// sigtermRun builds testdata/sigterm and runs it with args, answering its exit
// code and its combined output.
func sigtermRun(t *testing.T, args ...string) (int, string) {
	t.Helper()
	program := filepath.Join(t.TempDir(), "sigterm")
	build := exec.Command("go", "build", "-o", program, "./testdata/sigterm")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the program: %v\n%s", err, out)
	}
	run := exec.Command(program, args...)
	run.Env = append(os.Environ(),
		"FLEZHA_SOCKET_FILES_PATH="+t.TempDir(),
		"FLEZHA_TRANSPORT=uds",
		"FLEZHA_LOG_LEVEL=info")
	out, _ := run.CombinedOutput()
	return run.ProcessState.ExitCode(), string(out)
}

// The core's own handler is installed without SA_ONSTACK and chains to the Go
// runtime's, so the signal meant to drain the runner ends it some other way.
func TestTheCoresOwnHandlerDoesNotDrainAGoProcess(t *testing.T) {
	code, out := sigtermRun(t)
	t.Logf("exit %d:\n%s", code, out)
	if code == 0 || strings.Contains(out, "draining") {
		t.Fatalf("expected the process to end without the core's drain; it exited %d:\n%s", code, out)
	}
}

// With the signals handed over, the first SIGTERM runs the core's drain and
// the process exits with the status the drain gives.
func TestAHandedOverSigtermDrains(t *testing.T) {
	code, out := sigtermRun(t, "host")
	if code != 0 || !strings.Contains(out, "termination signal received (terminated); draining") {
		t.Fatalf("expected the core's drain and exit 0; got exit %d:\n%s", code, out)
	}
	if strings.Contains(out, "SA_ONSTACK") || strings.Contains(out, "goroutine ") {
		t.Fatalf("the Go runtime reported a crash:\n%s", out)
	}
}
