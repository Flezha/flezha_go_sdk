// Twins of the reference's crash-resume steps fixtures (the commit chain, the
// saga, the output identity, the branch, the diamond and the retry ladder,
// plus the run-once echo) under the same factory ids, tasklet names, channel
// names and output bytes as test/apps/rust_runner/src/steps.rs.
// fixtures_steps_loop.go carries the loop and bench families.
//
// Entry payloads and plugin frames are UTF-8 text: an entry names the text
// codec and a bare Output sends a string as it stands, so only an output that
// prefixes its value declares an encoder. A hop between two tasklets carries
// JSON.
package main

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	flezha "github.com/flezha/flezha_go_sdk"
)

// -- the release gate ---------------------------------------------------------

// blockUntilReleasedSteps parks this poller goroutine, never the core's
// driver, for as long as the runner lives unreleased.
func blockUntilReleasedSteps() {
	for !stepsReleased() {
		time.Sleep(time.Second)
	}
}

// holdAtSteps parks at point when this runner was booted to hold there.
func holdAtSteps(point string) {
	if os.Getenv("FLEZHA_STEPS_E2E_HOLD") == point {
		blockUntilReleasedSteps()
	}
}

// armedSteps is read at handler time: each scenario arms its own runner.
func armedSteps(name string) bool {
	_, ok := os.LookupEnv(name)
	return ok
}

// -- the shared relay and the three bases --------------------------------------

// stepsRun is a steps relay's settings, the duration backstop optional.
func stepsRun(factoryID string, run any, maxDurationMs int64, wire func(c *flezha.ChannelBuilder)) fixtureRelay {
	return fixtureRelay{
		spec:     flezha.RelaySpec{FactoryID: factoryID, Run: run, Durable: fixtureDurable(), MaxDurationMs: maxDurationMs},
		channels: wire,
	}
}

// perInputSteps is the per-input loop's run setting.
func perInputSteps() map[string]any {
	return map[string]any{"steps": map[string]any{"each": "per_input"}}
}

// prefixedSteps is an output encoder writing p before the value.
func prefixedSteps(p string) func(string) ([]byte, error) {
	return func(v string) ([]byte, error) { return []byte(p + v), nil }
}

// seenSteps is a durable step's state: a sorted, de-duplicated list, so a
// re-run after a resume folds to the state it already had and snapshot bytes
// stay deterministic.
type seenSteps struct {
	Items []string `json:"items"`
}

type insertSteps struct {
	Insert string `json:"insert"`
}

func (c insertSteps) ApplyTo(state *seenSteps) {
	if slices.Contains(state.Items, c.Insert) {
		return
	}
	items := append(slices.Clone(state.Items), c.Insert)
	slices.Sort(items)
	state.Items = items
}

// durableStepSteps is the reference's `durable_step!`: a step committing one
// idempotent change, its own name unless record says otherwise, then releasing
// the input as it stands unless release says otherwise. A gated step waits
// before its change and its emit.
type durableStepSteps struct {
	name    string
	record  func(in string) string
	gated   func(in string) bool
	release func(in string, ctx *flezha.Ctx[seenSteps, string]) error
}

func (s durableStepSteps) Spec() flezha.Spec { return flezha.Spec{Name: s.name} }

func (durableStepSteps) InitState(*string) seenSteps { return seenSteps{Items: []string{}} }

func (durableStepSteps) Changes() flezha.Changes[seenSteps] {
	return flezha.Changes[seenSteps]{"insert": insertSteps{}}
}

func (s durableStepSteps) Run(in string, ctx *flezha.Ctx[seenSteps, string]) error {
	if s.gated != nil && s.gated(in) {
		blockUntilReleasedSteps()
	}
	value := s.name
	if s.record != nil {
		value = s.record(in)
	}
	if err := ctx.Change(insertSteps{Insert: value}); err != nil {
		return err
	}
	if s.release != nil {
		return s.release(in, ctx)
	}
	return ctx.Emit(in)
}

// undoingStepSteps is a durable step that compensates. A crash-resumed
// compensation is handed no outputs, so the undo is emitted unconditionally;
// an empty undo emits nothing.
type undoingStepSteps struct {
	durableStepSteps
	compensationGated bool
	undo              string
}

func (s undoingStepSteps) Compensate(_ []string, ctx *flezha.Ctx[seenSteps, string]) error {
	if s.compensationGated {
		blockUntilReleasedSteps()
	}
	if s.undo == "" {
		return nil
	}
	return ctx.Emit(s.undo)
}

// failingStepSteps always fails, which is how a saga is made to unwind.
type failingStepSteps struct{ name string }

func (s failingStepSteps) Spec() flezha.Spec { return flezha.Spec{Name: s.name} }

func (s failingStepSteps) Run(string, *flezha.Ctx[flezha.NoState, flezha.NoOutput]) error {
	return errors.New(s.name + " always fails")
}

// -- echo_steps ------------------------------------------------------------------

type echoArgsSteps struct{}

func (echoArgsSteps) Spec() flezha.Spec { return flezha.Spec{Name: "echo_args"} }

func (echoArgsSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit("echo:" + in)
}

// -- typed_once_steps --------------------------------------------------------------
//
// A once steps relay whose entry decodes the payload's type; its tasklet prints
// what the entry decoded and completes with it.

type onceTicketSteps struct {
	Text string `json:"text"`
}

type oncePlacedSteps struct{}

func (oncePlacedSteps) Spec() flezha.Spec { return flezha.Spec{Name: "once_placed"} }

func (oncePlacedSteps) Run(in onceTicketSteps, ctx *flezha.Ctx[flezha.NoState, string]) error {
	fmt.Printf("once_ran %s\n", in.Text)
	if err := ctx.Complete(in.Text); err != nil {
		return err
	}
	return ctx.Emit("placed:" + in.Text)
}

// -- the commit/block chain ------------------------------------------------------

var commitASteps = durableStepSteps{name: "commit_a"}

type blockBSteps struct{}

func (blockBSteps) Spec() flezha.Spec { return flezha.Spec{Name: "block_b"} }

func (blockBSteps) Run(string, *flezha.Ctx[flezha.NoState, flezha.NoOutput]) error {
	blockUntilReleasedSteps()
	return nil
}

func commitBlockChainSteps(c *flezha.ChannelBuilder) {
	flezha.Into(flezha.Into(textIn(c, "in"), commitASteps).Named("mid"), blockBSteps{})
}

// -- effect_block_steps / effect_flow -----------------------------------------
//
// The input names a file the effect appends one line to per run, so a resume
// shows whether the call ran again.

func countRunSteps(path string) (string, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	if _, err := f.WriteString("ran\n"); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return "answer", nil
}

type effectBlockSteps struct{}

func (effectBlockSteps) Spec() flezha.Spec { return flezha.Spec{Name: "effect_block"} }

func (effectBlockSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, flezha.NoOutput]) error {
	if _, err := flezha.Effect(ctx, "model", func() (string, error) { return countRunSteps(in) }); err != nil {
		return err
	}
	blockUntilReleasedSteps()
	return nil
}

type effectOnceSteps struct{}

func (effectOnceSteps) Spec() flezha.Spec { return flezha.Spec{Name: "effect_once"} }

func (effectOnceSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, flezha.NoOutput]) error {
	_, err := flezha.Effect(ctx, "model", func() (string, error) { return countRunSteps(in) })
	return err
}

// -- the keyed commits -------------------------------------------------------------

type keySplitSteps struct{}

func (keySplitSteps) Spec() flezha.Spec { return flezha.Spec{Name: "key_split"} }

func (keySplitSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	for _, key := range strings.Split(in, ",") {
		if err := ctx.Emit(key); err != nil {
			return err
		}
	}
	return nil
}

func keyedCommitsSteps(c *flezha.ChannelBuilder) flezha.Stream[string] {
	keys := flezha.Into(textIn(c, "in"), keySplitSteps{}).Named("keys").KeyBy(func(key string) string { return key })
	return flezha.Into(keys, undoingStepSteps{durableStepSteps: durableStepSteps{name: "keyed_commit"}}).Named("committed")
}

// -- failed_stage_steps ----------------------------------------------------------

type failedStageSteps struct{}

func (failedStageSteps) Spec() flezha.Spec { return flezha.Spec{Name: "failed_stage"} }

func (failedStageSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	if in == "b" {
		return errors.New("failed_stage fails on b")
	}
	return ctx.Emit(in)
}

// -- branch_steps ----------------------------------------------------------------
//
// branch_a fans out to branch_b (then branch_d) and branch_c. branch_d parks,
// or fails with FLEZHA_BRANCH_FAIL armed, so the rollback unwinds both
// branches before the root they share.

type branchDSteps struct{}

func (branchDSteps) Spec() flezha.Spec { return flezha.Spec{Name: "branch_d"} }

func (branchDSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	if armedSteps("FLEZHA_BRANCH_FAIL") {
		return errors.New("branch_d failed")
	}
	blockUntilReleasedSteps()
	return ctx.Emit("d:" + in)
}

// emitPrefixedSteps is a durable step's release writing p before the input.
func emitPrefixedSteps(p string) func(in string, ctx *flezha.Ctx[seenSteps, string]) error {
	return func(in string, ctx *flezha.Ctx[seenSteps, string]) error { return ctx.Emit(p + in) }
}

// -- diamond_steps ---------------------------------------------------------------

type diamJoinSteps struct{}

func (diamJoinSteps) Spec() flezha.Spec { return flezha.Spec{Name: "diam_join"} }

func (diamJoinSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit("j:" + in)
}

// -- retry_steps -------------------------------------------------------------------
//
// retry_flaky fails its first FLEZHA_STEPS_RETRY_FAILS attempts, counted across
// the process: a retry re-runs the same instance's tasklet in place.

var retryAttemptsSteps atomic.Int64

type retryFlakySteps struct{}

func (retryFlakySteps) Spec() flezha.Spec { return flezha.Spec{Name: "retry_flaky"} }

func (retryFlakySteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	attempt := retryAttemptsSteps.Add(1)
	if attempt <= wireInt(os.Getenv("FLEZHA_STEPS_RETRY_FAILS")) {
		return fmt.Errorf("flaky attempt %d", attempt)
	}
	return ctx.Emit(fmt.Sprintf("attempt:%d:%s", attempt, in))
}

// -- timer_loop_steps / event_timer_steps --------------------------------------
//
// tick arms a processing-time timer for `after:<ms>` and emits `armed:<at>`;
// its firing emits `fired:<at>`. `slow:<ms>` holds tick that long. A runner
// booted with FLEZHA_STEPS_E2E_HOLD parks the firing at `timer` (inside
// on_timer) or `tock` (in tock).

type timerTickSteps struct{ durableStepSteps }

func (t timerTickSteps) Run(in string, ctx *flezha.Ctx[seenSteps, string]) error {
	if rest, ok := strings.CutPrefix(in, "after:"); ok {
		at, err := ctx.TimerAfter(uint64(wireInt(rest)))
		if err != nil {
			return err
		}
		armed := "armed:" + strconv.FormatUint(at, 10)
		if err := ctx.Change(insertSteps{Insert: armed}); err != nil {
			return err
		}
		return ctx.Emit(armed)
	}
	if rest, ok := strings.CutPrefix(in, "slow:"); ok {
		time.Sleep(time.Duration(wireInt(rest)) * time.Millisecond)
	}
	return ctx.Emit(in)
}

func (timerTickSteps) OnTimer(timer flezha.TimerFiring, ctx *flezha.Ctx[seenSteps, string]) error {
	holdAtSteps("timer")
	return ctx.Emit("fired:" + strconv.FormatUint(timer.ScheduledAt, 10))
}

type timerTockSteps struct{}

func (timerTockSteps) Spec() flezha.Spec { return flezha.Spec{Name: "tock"} }

func (timerTockSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	if strings.HasPrefix(in, "fired:") {
		holdAtSteps("tock")
	}
	return ctx.Emit("tock:" + in)
}

// eventTickSteps' clock is the message's leading event time: each message arms
// an event-time timer 100 later.
type eventTickSteps struct{ durableStepSteps }

func (eventTickSteps) EventTime(in string) uint64 { return uint64(wireInt(prefix(in))) }

func (eventTickSteps) Run(_ string, ctx *flezha.Ctx[seenSteps, string]) error {
	at, err := ctx.TimerAtEventTime(ctx.Now() + 100)
	if err != nil {
		return err
	}
	if err := ctx.Change(insertSteps{Insert: "armed:" + strconv.FormatUint(at, 10)}); err != nil {
		return err
	}
	return ctx.Emit("seen:" + strconv.FormatUint(ctx.Now(), 10))
}

func (eventTickSteps) OnTimer(timer flezha.TimerFiring, ctx *flezha.Ctx[seenSteps, string]) error {
	return ctx.Emit("fired:" + strconv.FormatUint(timer.ScheduledAt, 10))
}

// -- call_turn -------------------------------------------------------------------

type callTurnStepSteps struct{}

func (callTurnStepSteps) Spec() flezha.Spec { return flezha.Spec{Name: "call_turn"} }

func (callTurnStepSteps) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in)
}

// -- registration ------------------------------------------------------------------

func stepsFixtures() []flezha.Relay {
	return []flezha.Relay{
		stepsRun("echo_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			flezha.Into(textIn(c, "in"), echoArgsSteps{}).Output("out")
		}),
		stepsRun("typed_once_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			order := flezha.Input[onceTicketSteps](c, "order", flezha.InputOptions{Codec: flezha.JSON})
			flezha.Into(order, oncePlacedSteps{}).Output("placed")
		}),
		stepsRun("commit_block_steps", "steps", 0, commitBlockChainSteps),
		fixtureRelay{
			spec:     flezha.RelaySpec{FactoryID: "sync_durable_steps", Run: "steps", Durable: "remote"},
			channels: commitBlockChainSteps,
		},
		stepsRun("effect_block_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			flezha.Into(textIn(c, "in"), effectBlockSteps{})
		}),
		fixtureRelay{
			spec:     flezha.RelaySpec{FactoryID: "effect_flow"},
			channels: func(c *flezha.ChannelBuilder) { flezha.Into(textIn(c, "in"), effectOnceSteps{}) },
		},
		fixtureRelay{
			// The opt-out twin: commit_a's output also reaches a plugin output,
			// so a drill sees the mid-run point without durable state.
			spec: flezha.RelaySpec{FactoryID: "optout_commit_block", Run: "steps", Durable: false},
			channels: func(c *flezha.ChannelBuilder) {
				mid := flezha.Into(textIn(c, "in"), commitASteps).OutputEncoded("mid", prefixedSteps("a-ran:"))
				flezha.Into(mid, blockBSteps{})
			},
		},
		stepsRun("tier_commit_block_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			tierCommitA := durableStepSteps{name: "tier_commit_a", release: func(in string, ctx *flezha.Ctx[seenSteps, string]) error {
				return ctx.Emit("committed:"+in, flezha.WithKey("job"))
			}}
			flezha.Into(flezha.Into(textIn(c, "in"), tierCommitA).Keyed().Output("mid"), blockBSteps{})
		}),
		stepsRun("saga_block_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			sagaA := undoingStepSteps{durableStepSteps: durableStepSteps{name: "saga_a"}, compensationGated: true}
			sagaB := undoingStepSteps{durableStepSteps: durableStepSteps{name: "saga_b"}}
			s1 := flezha.Into(textIn(c, "in"), sagaA).Named("s1")
			flezha.Into(flezha.Into(s1, sagaB).Named("s2"), failingStepSteps{name: "saga_c"})
		}),
		stepsRun("keyed_block_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			flezha.Into(keyedCommitsSteps(c), blockBSteps{})
		}),
		stepsRun("keyed_saga_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			flezha.Into(keyedCommitsSteps(c), failingStepSteps{name: "keyed_fail"})
		}),
		stepsRun("outbox_block_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			outboxA := durableStepSteps{name: "commit_a", release: func(in string, ctx *flezha.Ctx[seenSteps, string]) error {
				return ctx.Emit(in, flezha.WithKey("k"), flezha.WithMetadata(map[string]string{"trace": "t1"}))
			}}
			mid := flezha.Into(textIn(c, "in"), outboxA).Keyed().OutputEncoded("mid", prefixedSteps("a-out:"))
			flezha.Into(mid, blockBSteps{})
		}),
		stepsRun("undo_outbox_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			undoOutboxA := undoingStepSteps{
				durableStepSteps: durableStepSteps{name: "undo_outbox_a"}, compensationGated: true, undo: "undone",
			}
			mid := flezha.Into(textIn(c, "in"), undoOutboxA).OutputEncoded("mid", prefixedSteps("out:"))
			flezha.Into(mid, failingStepSteps{name: "undo_outbox_c"})
		}),
		stepsRun("failed_stage_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			split := flezha.Into(textIn(c, "in"), keySplitSteps{})
			flezha.Into(split, failedStageSteps{}).OutputEncoded("mid", prefixedSteps("lost:"))
		}),
		stepsRun("retry_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			retryA := undoingStepSteps{durableStepSteps: durableStepSteps{name: "retry_a"}, undo: "undone"}
			mid := flezha.Into(textIn(c, "in"), retryA).OutputEncoded("mid", prefixedSteps("res:"))
			flezha.Into(mid, retryFlakySteps{}).Output("out")
		}),
		stepsRun("branch_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			branchA := undoingStepSteps{durableStepSteps: durableStepSteps{name: "branch_a"}, undo: "undo-a"}
			branchB := undoingStepSteps{durableStepSteps: durableStepSteps{name: "branch_b"}, undo: "undo-b"}
			branchC := undoingStepSteps{
				durableStepSteps: durableStepSteps{name: "branch_c", release: emitPrefixedSteps("c:")}, undo: "undo-c",
			}
			// One channel, two consumers: both branches read what branch_a released.
			mid := flezha.Into(textIn(c, "in"), branchA).OutputEncoded("mid", prefixedSteps("a:"))
			// Fail fast into the one tasklet that fails: the compensation
			// scenario is about the rollback order, not the ladder before it.
			bmid := flezha.Into(mid, branchB).OutputEncoded("bmid", prefixedSteps("b:")).RetryMax(1)
			flezha.Into(bmid, branchDSteps{}).Output("d_out")
			flezha.Into(mid, branchC).Output("c_out")
		}),
		stepsRun("diamond_steps", "steps", 0, func(c *flezha.ChannelBuilder) {
			diamB2 := durableStepSteps{
				name:    "diam_b2",
				gated:   func(string) bool { return true },
				record:  func(string) string { return "diam_b" },
				release: emitPrefixedSteps("b:"),
			}
			diamC := durableStepSteps{name: "diam_c", release: emitPrefixedSteps("c:")}
			// Two arms naming diam_join with no node name are one node: the fan-in.
			fan := flezha.Into(textIn(c, "in"), durableStepSteps{name: "diam_a"}).OutputEncoded("dfan", prefixedSteps("a:"))
			b1 := flezha.Into(fan, durableStepSteps{name: "diam_b1"}).Named("db1")
			flezha.Into(flezha.Into(b1, diamB2).Output("db2"), diamJoinSteps{})
			flezha.Into(flezha.Into(fan, diamC).Output("dc"), diamJoinSteps{}).Output("j_out")
		}),
		stepsRun("timer_loop_steps", perInputSteps(), 600_000, func(c *flezha.ChannelBuilder) {
			ticked := flezha.Into(textIn(c, "in"), timerTickSteps{durableStepSteps{name: "tick"}}).Named("ticked")
			flezha.Into(ticked, timerTockSteps{}).Output("out")
		}),
		stepsRun("event_timer_steps", perInputSteps(), 600_000, func(c *flezha.ChannelBuilder) {
			flezha.Into(textIn(c, "in"), eventTickSteps{durableStepSteps{name: "event_tick"}}).Output("out")
		}),
		stepsRun("call_turn", perInputSteps(), 600_000, func(c *flezha.ChannelBuilder) {
			flezha.Into(textIn(c, "in"), callTurnStepSteps{}).Output("")
		}),
	}
}
