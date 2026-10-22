package flezha_test

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/flezha/flezha_go_sdk/internal/contract"
)

// fakeController is the core's controller stand-in, built by scripts/build-go-sdk.sh.
func fakeController(t *testing.T) string {
	t.Helper()
	if path := os.Getenv("FLEZHA_FAKE_CONTROLLER"); path != "" {
		return path
	}
	dir, err := contract.Dir()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "_build", ".target", "debug", "flz-fake-controller")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no fake controller at %s (set FLEZHA_FAKE_CONTROLLER, or run scripts/build-go-sdk.sh): %v", path, err)
	}
	return path
}

var buildOnce sync.Once
var runnerPath string
var buildErr error

// testRunner is testdata/runner, built with the race detector.
func testRunner(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "flezha-go-runner")
		if err != nil {
			buildErr = err
			return
		}
		runnerPath = filepath.Join(dir, "runner")
		out, err := exec.Command("go", "build", "-race", "-o", runnerPath, "./testdata/runner").CombinedOutput()
		if err != nil {
			buildErr = &exec.ExitError{Stderr: out}
		}
	})
	if buildErr != nil {
		t.Fatalf("building testdata/runner: %v", buildErr)
	}
	return runnerPath
}

// cleanOf fails on anything in a runner's output that says the Go runtime or
// the race detector objected.
func cleanOf(t *testing.T, out string) {
	t.Helper()
	for _, sign := range []string{"DATA RACE", "SA_ONSTACK", "fatal error", "panic:", "cgo argument", "goroutine "} {
		if strings.Contains(out, sign) {
			t.Fatalf("the runner reported %q:\n%s", sign, out)
		}
	}
}

// smoke drives the test runner serving relay through the fake controller,
// answering its output and how long the run took.
func smoke(t *testing.T, relay string, inputs []string, outputs int, env ...string) (string, time.Duration) {
	t.Helper()
	args := []string{"--factory", relay, "--expect-outputs", itoa(outputs), "--timeout-ms", "30000"}
	for _, in := range inputs {
		args = append(args, "--input", in)
	}
	args = append(args, "--", testRunner(t), relay)
	cmd := exec.Command(fakeController(t), args...)
	cmd.Env = append(append(os.Environ(), "GODEBUG=cgocheck=1"), env...)
	started := time.Now()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the smoke run failed: %v\n%s", err, out)
	}
	return string(out), time.Since(started)
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestARunnerConnectsGetsWorkCompletesAndShutsDown(t *testing.T) {
	out, _ := smoke(t, "echo", []string{"a", "b", "c"}, 3)
	if !strings.Contains(out, "3 outputs seen") {
		t.Fatalf("the run did not see three outputs:\n%s", out)
	}
}

func TestBlockedHandlersBuyWorkers(t *testing.T) {
	inputs := []string{"k1", "k2", "k3", "k4", "k5", "k6"}
	_, took := smoke(t, "slow", inputs, len(inputs),
		"FLEZHA_WORKER_POOL_SIZE=1", "FLEZHA_WORKER_POOL_MAX=8", "SLOW_MS=1500")
	// One poller would take nine seconds; grown pollers serve the queued cells.
	if took > 7*time.Second {
		t.Fatalf("six held inputs took %s on a base pool of one; the pool did not grow", took)
	}
}

func TestADropNothingWedgeRecoversOnABasePoolOfOne(t *testing.T) {
	out, _ := smoke(t, "wedge", []string{"a", "b"}, 10, "FLEZHA_WORKER_POOL_SIZE=1")
	if !strings.Contains(out, "10 outputs seen") {
		t.Fatalf("the wedge did not drain:\n%s", out)
	}
}

// lockedBuffer collects a process's output from more than one goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// standalone starts the fake controller with a socket directory of its own
// and the test runner beside it, as two processes the test signals.
func standalone(t *testing.T, relay string, inputs []string, expect int, runnerEnv ...string) (controller *exec.Cmd, controllerOut *lockedBuffer, runner *exec.Cmd, stdin io.WriteCloser, lines chan string, runnerOut *lockedBuffer) {
	t.Helper()
	dir := t.TempDir()
	args := []string{"--socket-dir", dir, "--factory", relay, "--expect-outputs", itoa(expect), "--timeout-ms", "20000"}
	for _, in := range inputs {
		args = append(args, "--input", in)
	}
	controller = exec.Command(fakeController(t), args...)
	controllerOut = &lockedBuffer{}
	pipe, _ := controller.StdoutPipe()
	controller.Stderr = controllerOut
	if err := controller.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controller.Process.Kill(); _ = controller.Wait() })
	listening := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(pipe)
		once := sync.Once{}
		for scanner.Scan() {
			_, _ = controllerOut.Write([]byte(scanner.Text() + "\n"))
			if strings.Contains(scanner.Text(), "listening on") {
				once.Do(func() { close(listening) })
			}
		}
	}()
	select {
	case <-listening:
	case <-time.After(10 * time.Second):
		t.Fatal("the fake controller never listened")
	}

	runner = exec.Command(testRunner(t), relay)
	runner.Env = append(append(os.Environ(), "FLEZHA_SOCKET_FILES_PATH="+dir, "FLEZHA_TRANSPORT=uds", "GODEBUG=cgocheck=1"), runnerEnv...)
	stdin, _ = runner.StdinPipe()
	errPipe, _ := runner.StderrPipe()
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.Process.Kill() })
	lines = make(chan string, 256)
	runnerOut = &lockedBuffer{}
	go func() {
		scanner := bufio.NewScanner(errPipe)
		for scanner.Scan() {
			_, _ = runnerOut.Write([]byte(scanner.Text() + "\n"))
			lines <- scanner.Text()
		}
		close(lines)
	}()
	return
}

func await(t *testing.T, lines chan string, want string) {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("the runner ended before printing %q", want)
			}
			if strings.Contains(line, want) {
				return
			}
		case <-deadline:
			t.Fatalf("the runner never printed %q", want)
		}
	}
}

// exitWithin waits for the runner to exit, up to bound, answering its status.
func exitWithin(t *testing.T, runner *exec.Cmd, lines chan string, bound time.Duration) int {
	t.Helper()
	go func() {
		for range lines {
		}
	}()
	done := make(chan error, 1)
	go func() { done <- runner.Wait() }()
	select {
	case <-done:
		return runner.ProcessState.ExitCode()
	case <-time.After(bound):
		t.Fatalf("the runner was still running %s later", bound)
		return -1
	}
}

func TestSigtermDrainsInsteadOfCrashing(t *testing.T) {
	_, controllerOut, runner, _, lines, runnerOut := standalone(t, "slow", []string{"inflight"}, 2, "SLOW_MS=1500")
	await(t, lines, "handling inflight")
	if err := runner.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// The core's grace is five seconds.
	code := exitWithin(t, runner, lines, 8*time.Second)
	out := runnerOut.String()
	cleanOf(t, out)
	if code != 0 || !strings.Contains(out, "termination signal received (terminated); draining") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	// The controller waits for a second output it is never sent, so the
	// signal alone ends the runner; the first arrived before the exit.
	time.Sleep(500 * time.Millisecond)
	if !strings.Contains(controllerOut.String(), "after 1 of 2 outputs") {
		t.Fatalf("the activation in flight did not complete before the exit:\n%s", controllerOut.String())
	}
}

func TestClosingStandardInputStopsTheRunner(t *testing.T) {
	_, _, runner, stdin, lines, runnerOut := standalone(t, "echo", nil, 1)
	await(t, lines, "started")
	_ = stdin.Close()
	code := exitWithin(t, runner, lines, 8*time.Second)
	out := runnerOut.String()
	cleanOf(t, out)
	if code != 0 || !strings.Contains(out, "standard input reached end of file; draining") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}

func TestCoreNoticesReachTheApplicationsLogger(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "notices.log")
	cmd := exec.Command(testRunner(t), "echo")
	cmd.Env = append(os.Environ(), "NOTICE_LOG="+logPath, "FLEZHA_SOCKET_FILES_PATH="+t.TempDir(),
		"FLEZHA_TRANSPORT=uds", "FLEZHA_RUNNER_START_WAIT_MS=1500")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if len(out) != 0 {
		t.Fatalf("the runner wrote past its logger:\n%s", out)
	}
	logged, _ := os.ReadFile(logPath)
	if !strings.Contains(string(logged), "level=WARN") || !strings.Contains(string(logged), "Connection attempt") {
		t.Fatalf("the core's connection notices did not reach the logger:\n%s", logged)
	}
}

func TestZeroDeclaredRelayTypesIsLegal(t *testing.T) {
	cmd := exec.Command(fakeController(t), "--factory", "nothing_declared", "--expect-outputs", "0",
		"--timeout-ms", "10000", "--", testRunner(t), "none")
	out, _ := cmd.CombinedOutput()
	// The fake controller wants a factory the runner never declares, so it
	// stops after the connect frame: which proves the runner connected.
	if !strings.Contains(string(out), "connected without declaring `nothing_declared`; it declared") {
		t.Fatalf("the runner with no relay type did not connect:\n%s", out)
	}
}

func TestAFailedStartCanBeRetried(t *testing.T) {
	cmd := exec.Command(testRunner(t), "echo")
	cmd.Env = append(os.Environ(), "RETRY_START=1", "FLEZHA_SOCKET_FILES_PATH="+t.TempDir(),
		"FLEZHA_TRANSPORT=uds", "FLEZHA_RUNNER_START_WAIT_MS=500", "FLEZHA_LOG_LEVEL=error")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if strings.Count(string(out), "start refused: runtime.connect_unconfirmed") != 2 {
		t.Fatalf("the second start was not a fresh attempt:\n%s", out)
	}
}

func TestCloseShipsStateAndEndsAwaitTermination(t *testing.T) {
	_, _, runner, _, lines, runnerOut := standalone(t, "echo", []string{"a"}, 2, "CLOSE_AFTER_MS=1500")
	await(t, lines, "started")
	code := exitWithin(t, runner, lines, 10*time.Second)
	out := runnerOut.String()
	cleanOf(t, out)
	if code != 0 || !strings.Contains(out, "closed") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}

func TestASignalDuringTheConnectWaitDrains(t *testing.T) {
	cmd := exec.Command(testRunner(t), "echo")
	cmd.Env = append(os.Environ(), "FLEZHA_SOCKET_FILES_PATH="+t.TempDir(), "FLEZHA_TRANSPORT=uds",
		"FLEZHA_RUNNER_START_WAIT_MS=10000", "FLEZHA_LOG_LEVEL=info")
	out := &lockedBuffer{}
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	_ = cmd.Process.Signal(syscall.SIGTERM)
	_ = cmd.Wait()
	if code := cmd.ProcessState.ExitCode(); code != 0 || !strings.Contains(out.String(), "draining") {
		t.Fatalf("a SIGTERM during the connect wait exited %d:\n%s", code, out.String())
	}
}
