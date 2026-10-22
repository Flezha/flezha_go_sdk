package native

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/flezha/flezha_go_sdk/internal/contract"
)

// declared is every entry point the cgo preamble of native.go declares.
func declared(t *testing.T) []string {
	t.Helper()
	source, err := os.ReadFile("native.go")
	if err != nil {
		t.Fatal(err)
	}
	preamble := string(source)[:strings.Index(string(source), `import "C"`)]
	seen := map[string]bool{}
	for _, m := range regexp.MustCompile(`\b(flz_core_[a-z_]+)\(`).FindAllStringSubmatch(preamble, -1) {
		seen[m[1]] = true
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func exported(t *testing.T) map[string]bool {
	t.Helper()
	text, err := contract.Read("test/fixtures/abi_surface.txt")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(text), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			out[line] = true
		}
	}
	return out
}

func TestEveryDeclaredEntryIsInTheFixture(t *testing.T) {
	listed := exported(t)
	for _, name := range declared(t) {
		if !listed[name] {
			t.Errorf("native.go declares %s, which test/fixtures/abi_surface.txt does not list", name)
		}
	}
}

func TestEveryFixtureEntryIsDeclared(t *testing.T) {
	have := map[string]bool{}
	for _, name := range declared(t) {
		have[name] = true
	}
	for name := range exported(t) {
		if !have[name] {
			t.Errorf("the ABI exports %s and native.go does not declare it", name)
		}
	}
}

func TestASchemaMismatchIsRefusedNamingBothVersions(t *testing.T) {
	_, err := newContextAt(SchemaVersion + 1)
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != "runtime.schema_mismatch" {
		t.Fatalf("expected runtime.schema_mismatch, got %v", err)
	}
	for _, version := range []string{"8", "9"} {
		if !strings.Contains(refusal.Message, version) {
			t.Fatalf("the refusal does not name version %s: %s", version, refusal.Message)
		}
	}
}

func TestTheCompiledSchemaIsTheCores(t *testing.T) {
	if CoreSchemaVersion() != SchemaVersion {
		t.Fatalf("the linked core speaks %d, this package %d", CoreSchemaVersion(), SchemaVersion)
	}
}

func TestARefusalCarriesItsCodeThroughErrorsAs(t *testing.T) {
	ctx, err := NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer ctx.Free()
	err = ctx.DeclareClass([]byte(`{"factory_id":"x","sdk_lang":8,"sdk_version":"0.1.0","channels":[],"tasklets":[]}`))
	var refusal *Error
	if !errors.As(err, &refusal) || !strings.HasPrefix(refusal.Code, "topology.") || refusal.Class == "" {
		t.Fatalf("expected a topology refusal, got %#v", err)
	}
}

func TestARefusalTheSdkMakesReadsAsTheCores(t *testing.T) {
	refusal := Refusal("runtime.change_not_registered", map[string]any{"tasklet": "totals", "type": "main.Added"})
	if refusal.Code != "runtime.change_not_registered" || refusal.Class != "invalid" ||
		!strings.Contains(refusal.Message, "totals") || !strings.Contains(refusal.Message, "main.Added") {
		t.Fatalf("unexpected rendering: %#v", refusal)
	}
}

func TestBuffersComeBackAsGoMemory(t *testing.T) {
	ctx, err := NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer ctx.Free()
	ids, err := ctx.Relays()
	if err == nil && string(ids) != "[]" {
		t.Fatalf("an unstarted runner binds %s", ids)
	}
}

// Goroutines failing on one context at once each read their own refusal.
func TestConcurrentRefusalsAreEachTheirOwn(t *testing.T) {
	ctx, err := NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer ctx.Free()
	var wg sync.WaitGroup
	wrong := make(chan string, 16)
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				id := fmt.Sprintf("factory-%d-%d", g, i)
				var refusal *Error
				if err := ctx.UndeclareClass(id); !errors.As(err, &refusal) || !strings.Contains(refusal.Message, "'"+id+"'") {
					wrong <- fmt.Sprintf("%s read %v", id, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(wrong)
	for w := range wrong {
		t.Error(w)
	}
}
