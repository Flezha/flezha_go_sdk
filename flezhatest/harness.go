// Package flezhatest drives one tasklet without a controller, for an author's
// own tests: the SDK's own dispatch path over a one-tasklet relay, so what a
// test proves is what the runner does.
package flezhatest

import (
	"encoding/json"
	"fmt"
	"reflect"

	flezha "github.com/flezha/flezha_go_sdk"
	"github.com/flezha/flezha_go_sdk/internal/harness"
)

// TaskletTestHarness drives one cell of a tasklet. An Effect keeps its result
// on a live Feed; the n-th Feed with replay replays the n-th live input and is
// served what it kept, and fails when a function runs there or a kept result
// is never asked for.
type TaskletTestHarness[I, S, O any] struct {
	engine harness.Engine
}

// Outcome is what one invocation produced, as a test reads it.
type Outcome[S, O any] struct {
	Outputs []O
	Changes []flezha.Change[S]
	Timers  []Timer
}

// Timer is one timer operation an invocation recorded.
type Timer struct {
	Kind        flezha.TimerKind
	ScheduledAt uint64
	Register    bool
}

// Options name the cell a harness drives: its key (none for a single cell)
// and its relay instance.
type Options struct {
	CellKey *string
	Relay   string
}

// New is a harness over t: one entry channel into the tasklet, and one JSON
// output, so Outputs are exactly the values it emitted.
func New[I, S, O any](t flezha.Tasklet[I, S, O], opts ...Options) (*TaskletTestHarness[I, S, O], error) {
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Relay == "" {
		o.Relay = "harness"
	}
	engine, err := harness.New(oneTasklet[I, S, O]{t}, o.CellKey, o.Relay)
	if err != nil {
		return nil, err
	}
	return &TaskletTestHarness[I, S, O]{engine: engine}, nil
}

type oneTasklet[I, S, O any] struct{ t flezha.Tasklet[I, S, O] }

func (oneTasklet[I, S, O]) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "<harness>"} }

func (r oneTasklet[I, S, O]) Channels(c *flezha.ChannelBuilder) {
	flezha.Into(flezha.Input[I](c, "in"), r.t).Output("out", flezha.JSON)
}

// State is the cell's live state.
func (h *TaskletTestHarness[I, S, O]) State() S {
	state, _ := h.engine.State().(S)
	return state
}

// Feed runs the tasklet on one input at now; replay replays the next live
// input, in order.
func (h *TaskletTestHarness[I, S, O]) Feed(input I, now uint64, replay bool) (Outcome[S, O], error) {
	return h.outcome(h.engine.Feed(input, now, replay))
}

// FireTimer fires a cell timer; Now inside OnTimer is scheduledAt, as it is live.
func (h *TaskletTestHarness[I, S, O]) FireTimer(kind flezha.TimerKind, scheduledAt uint64) (Outcome[S, O], error) {
	return h.outcome(h.engine.FireTimer(kind == flezha.EventTime, scheduledAt))
}

// Compensate runs the declared compensation over recorded forward outputs.
func (h *TaskletTestHarness[I, S, O]) Compensate(outputs []O) (Outcome[S, O], error) {
	encoded := make([][]byte, 0, len(outputs))
	for _, out := range outputs {
		b, err := json.Marshal(out)
		if err != nil {
			return Outcome[S, O]{}, err
		}
		encoded = append(encoded, b)
	}
	return h.outcome(h.engine.Compensate(encoded))
}

// Restore rebuilds the state as a restore does, from a snapshot (nil for
// none) and a change log, through the dispatcher's own fold.
func (h *TaskletTestHarness[I, S, O]) Restore(snapshot []byte, changes [][]byte) error {
	return h.engine.Restore(snapshot, changes)
}

// ReplayEquals folds every recorded change into a fresh initial state and
// requires the answer to equal the live state. It catches a tasklet that
// changed its state without recording a change (a map write, an element of a
// slice) and a change whose ApplyTo is not a pure fold.
func (h *TaskletTestHarness[I, S, O]) ReplayEquals() error {
	replayed, err := h.engine.Replayed()
	if err != nil {
		return err
	}
	live := h.engine.State()
	if !reflect.DeepEqual(live, replayed) {
		return fmt.Errorf("cell %s: replaying the recorded change log against a fresh initial state "+
			"produced %+v, but the live state is %+v. Either the tasklet changed its state without "+
			"recording a change, or a change's ApplyTo is not a pure fold; both are lost on restart",
			h.engine.Cell(), replayed, live)
	}
	return nil
}

func (h *TaskletTestHarness[I, S, O]) outcome(raw harness.Outcome, err error) (Outcome[S, O], error) {
	if err != nil {
		return Outcome[S, O]{}, err
	}
	var out Outcome[S, O]
	for _, b := range raw.Outputs {
		var v O
		if err := json.Unmarshal(b, &v); err != nil {
			return out, err
		}
		out.Outputs = append(out.Outputs, v)
	}
	for _, b := range raw.Changes {
		change, err := h.engine.DecodeChange(b)
		if err != nil {
			return out, err
		}
		out.Changes = append(out.Changes, change.(flezha.Change[S]))
	}
	for _, t := range raw.Timers {
		kind := flezha.ProcessingTime
		if t.EventTime {
			kind = flezha.EventTime
		}
		out.Timers = append(out.Timers, Timer{Kind: kind, ScheduledAt: t.ScheduledAt, Register: t.Register})
	}
	return out, nil
}
