package flezha_test

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	flezha "github.com/flezha/flezha_go_sdk"
	"github.com/flezha/flezha_go_sdk/internal/contract"
)

type split struct{}

func (split) Spec() flezha.Spec { return flezha.Spec{Name: "split"} }

func (split) Run(line string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	for _, word := range strings.Fields(line) {
		if err := ctx.Emit(strings.ToLower(word)); err != nil {
			return err
		}
	}
	return nil
}

type seen int

func (s seen) ApplyTo(state *int) { *state = int(s) }

type tally struct{}

func (tally) Spec() flezha.Spec { return flezha.Spec{Name: "tally"} }

func (tally) Changes() flezha.Changes[int] { return flezha.Changes[int]{"seen": seen(0)} }

func (tally) Run(word string, ctx *flezha.Ctx[int, string]) error {
	count := ctx.State() + 1
	if err := ctx.Change(seen(count)); err != nil {
		return err
	}
	return ctx.Emit(word + " " + strconv.Itoa(count))
}

type wordCount struct{}

func (wordCount) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{FactoryID: "local_word_count_example"}
}

func (wordCount) Channels(c *flezha.ChannelBuilder) {
	lines := flezha.Input[string](c, "lines", flezha.InputOptions{Codec: flezha.Text})
	words := flezha.Into(lines, split{}).KeyBy(func(word string) string { return word })
	flezha.Into(words, tally{}).Keyed().Output("totals")
}

// scenario is the shared word-count fixture: its lines, and its totals as a multiset.
func scenario(t *testing.T) ([]string, map[string]int) {
	t.Helper()
	text, err := contract.Read("test/fixtures/local_mode_word_count.txt")
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	totals := map[string]int{}
	for _, row := range strings.Split(string(text), "\n") {
		if line, ok := strings.CutPrefix(row, "lines: "); ok {
			lines = append(lines, line)
		} else if total, ok := strings.CutPrefix(row, "totals: "); ok {
			totals[total]++
		}
	}
	return lines, totals
}

func TestTheWordCountExampleMatchesTheFixture(t *testing.T) {
	lines, expected := scenario(t)
	run, err := flezha.StartLocal(wordCount{})
	if err != nil {
		t.Fatal(err)
	}
	var released []flezha.LocalOutput
	if err := run.OnOutput("totals", func(out flezha.LocalOutput) { released = append(released, out) }); err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		if err := run.Feed("lines", []byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if err := run.RunUntilIdle(20 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := run.Close(); err != nil {
		t.Fatal(err)
	}
	counted := map[string]int{}
	for _, out := range released {
		counted[string(out.Payload)]++
	}
	if !equalCounts(counted, expected) {
		t.Fatalf("released %v, the fixture says %v", keys(counted), keys(expected))
	}
	for _, stamp := range []string{"flz.cell", "flz.seq", "flz.key"} {
		if _, ok := released[0].Metadata[stamp]; !ok {
			t.Fatalf("a keyed output carries %s: %v", stamp, released[0].Metadata)
		}
	}
}

func TestAChannelTheRelayDoesNotDeclareIsRefusedByName(t *testing.T) {
	run, err := flezha.StartLocal(wordCount{})
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	err = run.Feed("nope", []byte("1"))
	var refusal *flezha.Error
	if !errors.As(err, &refusal) || refusal.Code != "runtime.channel_undeclared" || !strings.Contains(refusal.Message, "nope") {
		t.Fatalf("expected runtime.channel_undeclared naming nope, got %v", err)
	}
}

func equalCounts(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func keys(m map[string]int) []string {
	out := []string{}
	for k, v := range m {
		out = append(out, k+"×"+strconv.Itoa(v))
	}
	sort.Strings(out)
	return out
}
