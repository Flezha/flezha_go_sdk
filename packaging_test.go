package flezha_test

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

func TestTheModuleRequiresNothingAndNamesTheFloor(t *testing.T) {
	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mod), "require") || !strings.Contains(string(mod), "\ngo 1.") {
		t.Fatalf("go.mod must declare a go floor and require nothing:\n%s", mod)
	}
}

func TestTheRunnerNeedsNoCoreLibraryAtRunTime(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("ldd is Linux's")
	}
	out, err := exec.Command("ldd", testRunner(t)).CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if strings.Contains(string(out), "flz_core_ffi") {
		t.Fatalf("the runner loads the core library at run time:\n%s", out)
	}
}

func TestAMissingLibraryNamesIt(t *testing.T) {
	cmd := exec.Command("go", "build", "-o", t.TempDir()+"/x", "./testdata/wrong_edge/right")
	cmd.Env = append(os.Environ(), "CGO_LDFLAGS=-L"+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "flz_core_ffi") {
		t.Fatalf("a link with no core library must fail naming flz_core_ffi: %v\n%s", err, out)
	}
}
