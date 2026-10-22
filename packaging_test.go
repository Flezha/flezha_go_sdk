package flezha_test

import (
	"os"
	"os/exec"
	"path/filepath"
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

// Every example is a module of its own whose go.mod names the SDK by its
// released version, so each is vetted and its tests run against this checkout
// as its README runs it before the module is published: through a workspace
// that replaces that version with this module.
func TestEveryExampleBuildsAndPassesItsTestsAgainstThisModule(t *testing.T) {
	sdk, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	examples, err := filepath.Glob("examples/*/go.mod")
	if err != nil || len(examples) == 0 {
		t.Fatalf("no example modules found: %v", err)
	}
	for _, mod := range examples {
		dir := filepath.Dir(mod)
		t.Run(filepath.Base(dir), func(t *testing.T) {
			work := t.TempDir()
			for _, args := range [][]string{
				{"work", "init", filepath.Join(sdk, dir)},
				{"work", "edit", "-replace", "github.com/flezha/flezha_go_sdk=" + sdk},
			} {
				cmd := exec.Command("go", args...)
				cmd.Dir = work
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, out)
				}
			}
			for _, args := range [][]string{{"vet", "./..."}, {"test", "./..."}} {
				cmd := exec.Command("go", args...)
				cmd.Dir = dir
				cmd.Env = append(os.Environ(), "GOWORK="+filepath.Join(work, "go.work"))
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("go %s in %s: %v\n%s", strings.Join(args, " "), dir, err, out)
				}
			}
		})
	}
}
