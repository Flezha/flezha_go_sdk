// The durable-loop steps twins (the chat loop with suspension, its stateless
// twin and the rewindable variant), the spawn requesters, and the chains and
// loop `mix bench.steps` drives: the reference's own partition
// (test/apps/rust_runner/src/steps_loop.rs). The gate and the bases are in
// fixtures_steps.go.
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	flezha "github.com/flezha/flezha_go_sdk"
)

// -- the durable loop family ----------------------------------------------------
//
// chat_loop_steps: chat_turn commits each turn to cell state and chat_reply
// answers it. The turn `mid-crash` parks while the release variable is unset,
// keyed on the payload so the park is deterministic across park/rehydrate
// cycles. The turn `bye` records nothing, passes on like any turn, and
// completes the instance with the number of turns recorded, as decimal text.

type chatTurnSteps struct{ durableStepSteps }

// chatTurn records the turn itself, so the record count is the number of
// distinct turns that ran: a re-run of a committed iteration would show up.
var chatTurn = chatTurnSteps{durableStepSteps{
	name:   "chat_turn",
	record: func(in string) string { return in },
	gated:  func(in string) bool { return in == "mid-crash" },
}}

func (t chatTurnSteps) Run(in string, ctx *flezha.Ctx[seenSteps, string]) error {
	if in != "bye" {
		return t.durableStepSteps.Run(in, ctx)
	}
	if err := ctx.Emit(in); err != nil {
		return err
	}
	return ctx.Complete(strconv.Itoa(len(ctx.State().Items)))
}

type chatReplySteps struct{}

func (chatReplySteps) Spec() flezha.Spec { return flezha.Spec{Name: "chat_reply"} }

func (chatReplySteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit("reply:" + in)
}

// chatReplyUndoableSteps keeps what it said in cell state, because a rewind's
// compensation is rebuilt from the journal and handed no outputs: the hook
// retracts the last thing said and pops it.
type chatReplyUndoableSteps struct{}

type saidStateSteps struct {
	Said []string `json:"said"`
}

type saidSteps struct {
	Said string `json:"said"`
}

func (c saidSteps) ApplyTo(state *saidStateSteps) {
	state.Said = append(state.Said[:len(state.Said):len(state.Said)], c.Said)
}

type retractedSteps struct {
	Retracted bool `json:"retracted"`
}

func (retractedSteps) ApplyTo(state *saidStateSteps) {
	if len(state.Said) > 0 {
		state.Said = state.Said[:len(state.Said)-1]
	}
}

func (chatReplyUndoableSteps) Spec() flezha.Spec { return flezha.Spec{Name: "chat_reply_undoable"} }

func (chatReplyUndoableSteps) InitState(*string) saidStateSteps {
	return saidStateSteps{Said: []string{}}
}

func (chatReplyUndoableSteps) Changes() flezha.Changes[saidStateSteps] {
	return flezha.Changes[saidStateSteps]{"said": saidSteps{}, "retracted": retractedSteps{}}
}

func (chatReplyUndoableSteps) Run(in string, ctx *flezha.Ctx[saidStateSteps, string]) error {
	if err := ctx.Change(saidSteps{Said: in}); err != nil {
		return err
	}
	return ctx.Emit("reply:" + in)
}

func (chatReplyUndoableSteps) Compensate(_ []string, ctx *flezha.Ctx[saidStateSteps, string]) error {
	said := ctx.State().Said
	if len(said) == 0 {
		return nil
	}
	turn := said[len(said)-1]
	if err := ctx.Change(retractedSteps{Retracted: true}); err != nil {
		return err
	}
	return ctx.Emit("retract:" + turn)
}

// keyed_loop_steps: a loop keyed on its entry edge by the text before the
// first `:`, so one cell counts one symbol's messages and emits
// `<symbol>:<count>`.

type keyedLoopCountSteps struct{}

type countStateSteps struct {
	Value int64 `json:"value"`
}

type deltaSteps struct {
	Delta int64 `json:"delta"`
}

func (c deltaSteps) ApplyTo(state *countStateSteps) { state.Value += c.Delta }

func (keyedLoopCountSteps) Spec() flezha.Spec { return flezha.Spec{Name: "keyed_loop_count"} }

func (keyedLoopCountSteps) InitState(*string) countStateSteps { return countStateSteps{} }

func (keyedLoopCountSteps) Changes() flezha.Changes[countStateSteps] {
	return flezha.Changes[countStateSteps]{"delta": deltaSteps{}}
}

func (keyedLoopCountSteps) Run(in string, ctx *flezha.Ctx[countStateSteps, string]) error {
	if err := ctx.Change(deltaSteps{Delta: 1}); err != nil {
		return err
	}
	return ctx.Emit(prefix(in) + ":" + strconv.FormatInt(ctx.State().Value, 10))
}

// failing_turn_loop_steps: `note` records each message and its undo moves the
// latest to `undone`; `answer` replies `re:<message>`, and fails on `boom`
// always, on `boom-once` while unreleased, and on `boom-until-healed` until a
// `heal` turn set its state. failing_turn_undo_all_steps is the same under
// all-or-nothing.

type noteSteps struct{}

type notesStateSteps struct {
	Notes  []string `json:"notes"`
	Undone []string `json:"undone"`
}

type notedSteps struct {
	Noted string `json:"noted"`
}

func (c notedSteps) ApplyTo(state *notesStateSteps) {
	state.Notes = append(state.Notes[:len(state.Notes):len(state.Notes)], c.Noted)
}

type undoneSteps struct {
	Undone bool `json:"undone"`
}

func (undoneSteps) ApplyTo(state *notesStateSteps) {
	if len(state.Notes) == 0 {
		return
	}
	last := state.Notes[len(state.Notes)-1]
	state.Notes = state.Notes[:len(state.Notes)-1]
	state.Undone = append(state.Undone[:len(state.Undone):len(state.Undone)], last)
}

func (noteSteps) Spec() flezha.Spec { return flezha.Spec{Name: "note"} }

func (noteSteps) InitState(*string) notesStateSteps {
	return notesStateSteps{Notes: []string{}, Undone: []string{}}
}

func (noteSteps) Changes() flezha.Changes[notesStateSteps] {
	return flezha.Changes[notesStateSteps]{"noted": notedSteps{}, "undone": undoneSteps{}}
}

func (noteSteps) Run(in string, ctx *flezha.Ctx[notesStateSteps, string]) error {
	if err := ctx.Change(notedSteps{Noted: in}); err != nil {
		return err
	}
	return ctx.Emit(in)
}

func (noteSteps) Compensate(_ []string, ctx *flezha.Ctx[notesStateSteps, string]) error {
	return ctx.Change(undoneSteps{Undone: true})
}

type answerSteps struct{}

type healedStateSteps struct {
	Healed bool `json:"healed"`
}

type healedSteps struct {
	Healed bool `json:"healed"`
}

func (healedSteps) ApplyTo(state *healedStateSteps) { state.Healed = true }

func (answerSteps) Spec() flezha.Spec { return flezha.Spec{Name: "answer"} }

func (answerSteps) InitState(*string) healedStateSteps { return healedStateSteps{} }

func (answerSteps) Changes() flezha.Changes[healedStateSteps] {
	return flezha.Changes[healedStateSteps]{"healed": healedSteps{}}
}

func (answerSteps) Run(in string, ctx *flezha.Ctx[healedStateSteps, string]) error {
	fails := in == "boom" ||
		(in == "boom-once" && !stepsReleased()) ||
		(in == "boom-until-healed" && !ctx.State().Healed)
	if fails {
		return errors.New("answer refused " + in)
	}
	if in == "heal" {
		if err := ctx.Change(healedSteps{Healed: true}); err != nil {
			return err
		}
	}
	return ctx.Emit("re:" + in)
}

func failingTurnLoopSteps(c *flezha.ChannelBuilder) {
	noted := flezha.Into(textIn(c, "in"), noteSteps{}).Named("noted")
	flezha.Into(noted, answerSteps{}).Output("answer")
}

// blockingReplySteps blocks on the turn `reply-crash` while unreleased, after
// the root committed, so a kill lands between the root's commit and the pass's.
type blockingReplySteps struct{}

func (blockingReplySteps) Spec() flezha.Spec { return flezha.Spec{Name: "blocking_reply"} }

func (blockingReplySteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	if in == "reply-crash" {
		blockUntilReleasedSteps()
	}
	return ctx.Emit("reply:" + in)
}

// chatLoopOfSteps is `in -> chat_turn (mid) -> reply -> out`.
func chatLoopOfSteps[S any](reply flezha.Tasklet[string, S, string]) func(c *flezha.ChannelBuilder) {
	return func(c *flezha.ChannelBuilder) {
		mid := flezha.Into(textIn(c, "in"), chatTurn).Named("mid")
		flezha.Into(mid, reply).Output("out")
	}
}

// -- the spawn requesters ----------------------------------------------------------
//
// A handle's answer is observable nowhere but in the tasklet that read it, so
// each prints it as `spawn_result <label> <id> <outcome>`. These relays are
// registered with the core fixtures, in their order, not by this file.

// reportSpawnSteps prints a spawn handle's answer, or the refusal's code.
func reportSpawnSteps(label string, handle *flezha.SpawnHandle) {
	result, err := handle.Wait()
	if err != nil {
		code := err.Error()
		var refused *flezha.Error
		if errors.As(err, &refused) {
			code = refused.Code
		}
		fmt.Printf("spawn_result %s error %s\n", label, code)
		return
	}
	fmt.Printf("spawn_result %s %s %s\n", label, result.ID, result.Outcome)
}

// spawnRequesterSteps spawns stateless_loop_steps once with no id and once
// under the input as its id.
type spawnRequesterSteps struct{}

func (spawnRequesterSteps) Spec() flezha.Spec { return flezha.Spec{Name: "spawn_requester"} }

func (spawnRequesterSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	unnamed, err := ctx.SpawnSteps("stateless_loop_steps", in)
	if err != nil {
		return err
	}
	named, err := ctx.SpawnSteps("stateless_loop_steps", in, flezha.SpawnID(in))
	if err != nil {
		return err
	}
	reportSpawnSteps("unnamed", unnamed)
	reportSpawnSteps("named", named)
	return nil
}

// spawnHoldRequesterSteps spawns, prints the answer, then blocks, so a runner
// killed there dies with the spawn answered and its input not durably consumed.
type spawnHoldRequesterSteps struct{}

func (spawnHoldRequesterSteps) Spec() flezha.Spec { return flezha.Spec{Name: "spawn_hold_requester"} }

func (spawnHoldRequesterSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	handle, err := ctx.SpawnSteps("stateless_loop_steps", in)
	if err != nil {
		return err
	}
	reportSpawnSteps("hold", handle)
	blockUntilReleasedSteps()
	return nil
}

// spawnOnceRequesterSteps spawns typed_once_steps with the input inside the
// JSON object its entry channel decodes.
type spawnOnceRequesterSteps struct{}

func (spawnOnceRequesterSteps) Spec() flezha.Spec { return flezha.Spec{Name: "spawn_once_requester"} }

func (spawnOnceRequesterSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	handle, err := ctx.SpawnSteps("typed_once_steps", onceTicketSteps{Text: in})
	if err != nil {
		return err
	}
	reportSpawnSteps("once", handle)
	return nil
}

var spawnRequesterRelay = fixtureRelay{
	spec: flezha.RelaySpec{FactoryID: "spawn_requester"},
	channels: func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "in"), spawnRequesterSteps{}).Output("out")
	},
}

var spawnHoldRequesterRelay = fixtureRelay{
	spec: flezha.RelaySpec{
		FactoryID: "spawn_hold_requester",
		Run:       map[string]any{"flow": map[string]any{"exactly": "effectively_once"}},
	},
	channels: func(c *flezha.ChannelBuilder) {
		in := textIn(c, "in").OnFull(flezha.DropNothing)
		flezha.Into(in, spawnHoldRequesterSteps{}).Keyed().Output("out")
	},
}

var spawnOnceRequesterRelay = fixtureRelay{
	spec: flezha.RelaySpec{FactoryID: "spawn_once_requester"},
	channels: func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "in"), spawnOnceRequesterSteps{}).Output("out")
	},
}

// -- the bench chains --------------------------------------------------------------
//
// What `mix bench.steps --scenario chain` drives: `steps` durable pass-through
// steps in a line, the payload unchanged. The edge names follow the reference,
// which spells them from the step type rather than the tasklet name.

func benchChainSteps(steps int) func(c *flezha.ChannelBuilder) {
	return func(c *flezha.ChannelBuilder) {
		edge := flezha.Into(textIn(c, "in"), durableStepSteps{name: "bench_s0"})
		for at := 1; at < steps; at++ {
			edge = flezha.Into(edge.Named(fmt.Sprintf("edge_BenchS%d", at)), durableStepSteps{name: fmt.Sprintf("bench_s%d", at)})
		}
		edge.Output("out")
	}
}

// -- bench_loop_steps -----------------------------------------------------------
//
// The long-lived loop the `turns` scenario drives, with a bounded per-turn
// state. With FLEZHA_BENCH_WAKE_AFTER_MS set each turn arms a timer that far
// out, whose firing sends `wake:<fired_at>`.

type benchTurnSteps struct{}

type benchTurnStateSteps struct {
	Turns int64  `json:"turns"`
	Last  string `json:"last"`
}

type turnSteps struct {
	Turn string `json:"turn"`
}

func (c turnSteps) ApplyTo(state *benchTurnStateSteps) {
	state.Turns++
	state.Last = c.Turn
}

func (benchTurnSteps) Spec() flezha.Spec { return flezha.Spec{Name: "bench_turn"} }

func (benchTurnSteps) InitState(*string) benchTurnStateSteps { return benchTurnStateSteps{} }

func (benchTurnSteps) Changes() flezha.Changes[benchTurnStateSteps] {
	return flezha.Changes[benchTurnStateSteps]{"turn": turnSteps{}}
}

func (benchTurnSteps) Run(in string, ctx *flezha.Ctx[benchTurnStateSteps, string]) error {
	if err := ctx.Change(turnSteps{Turn: in}); err != nil {
		return err
	}
	if after, err := strconv.ParseUint(os.Getenv("FLEZHA_BENCH_WAKE_AFTER_MS"), 10, 64); err == nil {
		if _, err := ctx.TimerAfter(after); err != nil {
			return err
		}
	}
	return ctx.Emit(in)
}

func (benchTurnSteps) OnTimer(timer flezha.TimerFiring, ctx *flezha.Ctx[benchTurnStateSteps, string]) error {
	return ctx.Emit("wake:" + strconv.FormatUint(timer.ScheduledAt, 10))
}

// -- static_cell_steps ----------------------------------------------------------
//
// One tasklet declares three static cells and routes every message to two of
// them; each emits `<cell key>:<message>`.

type staticCellStepSteps struct{}

func (staticCellStepSteps) Spec() flezha.Spec {
	return flezha.Spec{Name: "static_cell_step", CellKeys: []string{"a", "b", "c"}}
}

func (staticCellStepSteps) CellsFor(string) []string { return []string{"a", "b"} }

func (staticCellStepSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	key, _ := ctx.CellKey()
	return ctx.Emit(key + ":" + in)
}

// -- complete_flow / chained_steps ------------------------------------------------

// completeFlowSteps echoes each input and ends its instance on `bye`, the echo
// of `bye` included.
type completeFlowSteps struct{}

func (completeFlowSteps) Spec() flezha.Spec { return flezha.Spec{Name: "complete_flow_tasklet"} }

func (completeFlowSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	if in == "bye" {
		if err := ctx.Complete(in); err != nil {
			return err
		}
	}
	return ctx.Emit(in)
}

// chainedCountdownSteps prints `chained_pass <n>` and hands the next pass
// `n - 1` on its own channel; the pass at zero hands nothing, and the empty
// batch ends the loop.
type chainedCountdownSteps struct{}

func (chainedCountdownSteps) Spec() flezha.Spec { return flezha.Spec{Name: "chained_countdown"} }

func (chainedCountdownSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	fmt.Printf("chained_pass %s\n", in)
	if n := wireInt(in); n > 0 {
		return ctx.Emit(strconv.FormatInt(n-1, 10))
	}
	return nil
}

// -- registration ------------------------------------------------------------------

func stepsLoopFixtures() []flezha.Relay {
	return []flezha.Relay{
		// A chat session ends on `bye`; the duration backstop keeps a stray
		// instance from outliving the test run.
		stepsRun("chat_loop_steps", perInputSteps(), 600_000, chatLoopOfSteps[flezha.NoState](chatReplySteps{})),
		stepsRun("stateless_loop_steps", perInputSteps(), 600_000, func(c *flezha.ChannelBuilder) {
			flezha.Into(textIn(c, "in"), chatReplySteps{}).Output("out")
		}),
		stepsRun("keyed_loop_steps", perInputSteps(), 600_000, func(c *flezha.ChannelBuilder) {
			trades := flezha.Input[string](c, "trades", flezha.InputOptions{Codec: flezha.Text}).KeyBy(prefix)
			flezha.Into(trades, keyedLoopCountSteps{}).Output("keyed_loop_steps_out")
		}),
		stepsRun("chat_rewind_steps", perInputSteps(), 600_000, chatLoopOfSteps[saidStateSteps](chatReplyUndoableSteps{})),
		stepsRun("failing_turn_loop_steps",
			map[string]any{"steps": map[string]any{"each": "per_input", "on_failure": "keep_progress"}},
			600_000, failingTurnLoopSteps),
		stepsRun("failing_turn_undo_all_steps", perInputSteps(), 600_000, failingTurnLoopSteps),
		stepsRun("reply_block_loop_steps", perInputSteps(), 600_000, chatLoopOfSteps[flezha.NoState](blockingReplySteps{})),
		stepsRun("bench_chain_1", "steps", 0, benchChainSteps(1)),
		stepsRun("bench_chain_4", "steps", 0, benchChainSteps(4)),
		stepsRun("bench_chain_16", "steps", 0, benchChainSteps(16)),
		fixtureRelay{
			spec:     flezha.RelaySpec{FactoryID: "bench_chain_optout_4", Run: "steps", Durable: false},
			channels: benchChainSteps(4),
		},
		// A day, not the chat loop's ten minutes: a soak outlives 600 s.
		stepsRun("bench_loop_steps", perInputSteps(), 24*3_600_000, func(c *flezha.ChannelBuilder) {
			mid := flezha.Into(textIn(c, "in"), benchTurnSteps{}).Named("mid")
			flezha.Into(mid, chatReplySteps{}).Output("out")
		}),
		stepsRun("static_cell_steps", perInputSteps(), 600_000, func(c *flezha.ChannelBuilder) {
			flezha.Into(textIn(c, "in"), staticCellStepSteps{}).Output("out")
		}),
		fixtureRelay{
			spec: flezha.RelaySpec{FactoryID: "complete_flow"},
			channels: func(c *flezha.ChannelBuilder) {
				in := flezha.Input[string](c, "input", flezha.InputOptions{Codec: flezha.Text})
				flezha.Into(in, completeFlowSteps{}).Output("complete_flow_out")
			},
		},
		fixtureRelay{
			spec: flezha.RelaySpec{FactoryID: "chained_steps", Run: map[string]any{"steps": map[string]any{"each": "chained"}}},
			channels: func(c *flezha.ChannelBuilder) {
				flezha.Into(textIn(c, "in"), chainedCountdownSteps{}).Named("carry")
			},
		},
	}
}
