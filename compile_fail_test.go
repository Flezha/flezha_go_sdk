package flezha_test

import (
	"os/exec"
	"strings"
	"testing"
)

// buildFixture builds one program under testdata/wrong_edge.
func buildFixture(t *testing.T, fixture string) (string, error) {
	t.Helper()
	out, err := exec.Command("go", "build", "-o", t.TempDir()+"/x", "./testdata/wrong_edge/"+fixture).CombinedOutput()
	return string(out), err
}

// A wrong edge fails `go build`, and the compiler's message names both sides:
// the stream's element type, and the tasklet or function that cannot take it.
func TestAMismatchedEdgeDoesNotCompile(t *testing.T) {
	for fixture, types := range map[string][]string{
		"into":  {"string", "Totals"},
		"merge": {"Quote", "Totals"},
		"map":   {"string", "int"},
	} {
		t.Run(fixture, func(t *testing.T) {
			out, err := buildFixture(t, fixture)
			if err == nil {
				t.Fatal("the wrong edge compiled")
			}
			for _, name := range types {
				if !strings.Contains(out, name) {
					t.Fatalf("the compiler's message does not name %s:\n%s", name, out)
				}
			}
		})
	}
}

// The into fixture with its edge right compiles, so the refusals above are the
// wrong edge's alone.
func TestARightEdgeCompiles(t *testing.T) {
	if out, err := buildFixture(t, "right"); err != nil {
		t.Fatalf("the right edge did not compile: %v\n%s", err, out)
	}
}
