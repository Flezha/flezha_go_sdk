package flezhatest_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	flezha "github.com/flezha/flezha_go_sdk"
	"github.com/flezha/flezha_go_sdk/flezhatest"
)

type book struct {
	Sum   int                 `json:"sum"`
	Lists map[string][]string `json:"lists"`
}

type added struct {
	Qty int `json:"qty"`
}

func (a added) ApplyTo(b *book) { b.Sum += a.Qty }

type order struct {
	Qty  int    `json:"qty"`
	Note string `json:"note"`
}

// totals records each order's quantity and emits the running total. A note
// "sneak" appends to a slice inside its state without a change; "assign"
// assigns a field of the state copy; "unlisted" records a change it does not
// list; "silent" emits nothing.
type totals struct{}

func (totals) Spec() flezha.Spec { return flezha.Spec{Name: "totals"} }

func (totals) InitState(*string) book { return book{Lists: map[string][]string{}} }

func (totals) Changes() flezha.Changes[book] { return flezha.Changes[book]{"added": added{}} }

type unlisted struct{}

func (unlisted) ApplyTo(*book) {}

func (totals) Run(in order, ctx *flezha.Ctx[book, int]) error {
	switch in.Note {
	case "sneak":
		state := ctx.State()
		state.Lists["seen"] = append(state.Lists["seen"], "x")
		return nil
	case "assign":
		state := ctx.State()
		state.Sum = 1000
		return nil
	case "unlisted":
		return ctx.Change(unlisted{})
	case "silent":
		return nil
	case "timer":
		_, err := ctx.TimerAfter(500)
		return err
	}
	if err := ctx.Change(added{Qty: in.Qty}); err != nil {
		return err
	}
	return ctx.Emit(ctx.State().Sum)
}

func (totals) OnTimer(t flezha.TimerFiring, ctx *flezha.Ctx[book, int]) error {
	return ctx.Emit(int(t.ScheduledAt) + map[flezha.TimerKind]int{flezha.ProcessingTime: 1, flezha.EventTime: 2}[t.Kind])
}

func harness(t *testing.T) *flezhatest.TaskletTestHarness[order, book, int] {
	t.Helper()
	h, err := flezhatest.New[order, book, int](totals{})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestAHarnessTestDrivesAStatefulTasklet(t *testing.T) {
	h := harness(t)
	for i, qty := range []int{2, 3} {
		out, err := h.Feed(order{Qty: qty}, 1000, false)
		if err != nil {
			t.Fatal(err)
		}
		want := []int{2, 5}[i]
		if len(out.Outputs) != 1 || out.Outputs[0] != want || h.State().Sum != want {
			t.Fatalf("input %d: outputs %v, state %+v; want %d", i, out.Outputs, h.State(), want)
		}
		if len(out.Changes) != 1 || out.Changes[0] != (added{Qty: qty}) {
			t.Fatalf("input %d recorded %v", i, out.Changes)
		}
	}
	if err := h.ReplayEquals(); err != nil {
		t.Fatal(err)
	}
}

func TestATimerFiresOnDemand(t *testing.T) {
	h := harness(t)
	out, err := h.Feed(order{Note: "timer"}, 1000, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Timers) != 1 || out.Timers[0] != (flezhatest.Timer{Kind: flezha.ProcessingTime, ScheduledAt: 1500, Register: true}) {
		t.Fatalf("registered %+v", out.Timers)
	}
	fired, err := h.FireTimer(flezha.ProcessingTime, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(fired.Outputs) != 1 || fired.Outputs[0] != 1501 {
		t.Fatalf("OnTimer emitted %v, want the firing's time and kind", fired.Outputs)
	}
}

func TestTheReplayCheckFindsAnUnrecordedSliceAppend(t *testing.T) {
	h := harness(t)
	if _, err := h.Feed(order{Qty: 1}, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Feed(order{Note: "sneak"}, 0, false); err != nil {
		t.Fatal(err)
	}
	err := h.ReplayEquals()
	if err == nil || !strings.Contains(err.Error(), "cell totals") || !strings.Contains(err.Error(), "seen:[x]") {
		t.Fatalf("expected the check to fail naming the cell and the differing state, got %v", err)
	}
}

func TestAssigningAFieldOfTheCopyChangesNothing(t *testing.T) {
	h := harness(t)
	if _, err := h.Feed(order{Qty: 4}, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Feed(order{Note: "assign"}, 0, false); err != nil {
		t.Fatal(err)
	}
	if h.State().Sum != 4 {
		t.Fatalf("the next input sees %+v; assigning the copy changed durable state", h.State())
	}
	if err := h.ReplayEquals(); err != nil {
		t.Fatal(err)
	}
}

func TestAnUnlistedChangeIsRefused(t *testing.T) {
	h := harness(t)
	_, err := h.Feed(order{Note: "unlisted"}, 0, false)
	var refusal *flezha.Error
	if !errors.As(err, &refusal) || refusal.Code != "runtime.change_not_registered" ||
		!strings.Contains(refusal.Message, "totals") || !strings.Contains(refusal.Message, "unlisted") {
		t.Fatalf("expected runtime.change_not_registered naming the type and the tasklet, got %v", err)
	}
}

func TestATaskletThatEmitsNothing(t *testing.T) {
	out, err := harness(t).Feed(order{Note: "silent"}, 0, false)
	if err != nil || len(out.Outputs) != 0 {
		t.Fatalf("emitted %v (%v)", out.Outputs, err)
	}
}

// renamed is added under another Go name, listed under the same change name.
type renamed struct {
	Qty int `json:"qty"`
}

func (r renamed) ApplyTo(b *book) { b.Sum += r.Qty }

type renamedTotals struct{ totals }

func (renamedTotals) Changes() flezha.Changes[book] { return flezha.Changes[book]{"added": renamed{}} }

func TestARenamedChangeTypeStillRestores(t *testing.T) {
	before := harness(t)
	if _, err := before.Feed(order{Qty: 7}, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err := before.Feed(order{Qty: 5}, 0, false); err != nil {
		t.Fatal(err)
	}
	log := [][]byte{[]byte(`{"c":"added","v":{"qty":7}}`), []byte(`{"c":"added","v":{"qty":5}}`)}
	after, err := flezhatest.New[order, book, int](renamedTotals{})
	if err != nil {
		t.Fatal(err)
	}
	if err := after.Restore(nil, log); err != nil {
		t.Fatal(err)
	}
	if after.State().Sum != before.State().Sum {
		t.Fatalf("restored %+v, the live run reached %+v", after.State(), before.State())
	}
}

// charge calls the outside once per input through Effect, and reads Now twice.
type charge struct{ calls *int }

func (charge) Spec() flezha.Spec { return flezha.Spec{Name: "charge"} }

func (c charge) Run(in int, ctx *flezha.Ctx[flezha.NoState, string]) error {
	first := ctx.Now()
	receipt, err := flezha.Effect(ctx, "charge", func() (string, error) {
		*c.calls++
		return "receipt-" + ctx.StableID(), nil
	})
	if err != nil {
		return err
	}
	if ctx.Now() != first {
		return errors.New("now moved within one invocation")
	}
	return ctx.Emit(receipt)
}

func TestARecordedEffectDoesNotRunTwiceAndNowIsConstant(t *testing.T) {
	calls := 0
	h, err := flezhatest.New[int, flezha.NoState, string](charge{calls: &calls})
	if err != nil {
		t.Fatal(err)
	}
	live, err := h.Feed(1, 1000, false)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := h.Feed(1, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("the function ran %d times; a replay is served the kept result", calls)
	}
	if live.Outputs[0] != replayed.Outputs[0] {
		t.Fatalf("the replay read %v, the live run %v", replayed.Outputs, live.Outputs)
	}
}

// fetch keeps an untyped result, as a call returning JSON, text or bytes would.
type fetch struct{ calls *int }

func (fetch) Spec() flezha.Spec { return flezha.Spec{Name: "fetch"} }

func (f fetch) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	got, err := flezha.Effect(ctx, "fetch", func() (any, error) {
		*f.calls++
		return in, nil
	})
	if err != nil {
		return err
	}
	return ctx.Emit(got.(string))
}

func TestAnUntypedEffectResultReadsBackAsText(t *testing.T) {
	calls := 0
	h, err := flezhatest.New[string, flezha.NoState, string](fetch{calls: &calls})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Feed("hello", 0, false); err != nil {
		t.Fatal(err)
	}
	replayed, err := h.Feed("hello", 0, true)
	if err != nil || calls != 1 || replayed.Outputs[0] != "hello" {
		t.Fatalf("replay read %v (%v) after %d calls", replayed.Outputs, err, calls)
	}
}

type name string

func (n name) String() string { return string(n) }

type greet struct{}

func (greet) Spec() flezha.Spec { return flezha.Spec{Name: "greet"} }

func (greet) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	got, err := flezha.Effect(ctx, "who", func() (fmt.Stringer, error) { return name(in), nil })
	if err != nil {
		return err
	}
	return ctx.Emit(got.String())
}

// A result type with methods cannot hold kept text, so the kept result is
// refused as JSON rather than converted into a value of another type.
func TestAMethodBearingEffectResultIsRefusedNotConverted(t *testing.T) {
	h, err := flezhatest.New[string, flezha.NoState, string](greet{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Feed("bob", 0, false); err == nil || strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected a decode refusal, got %v", err)
	}
}
