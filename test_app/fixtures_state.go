package main

// Fixtures whose subject is persisted Cell state: the keyed upsert the bench
// shares, the counter and resume twins and the timer cap, the same partition
// as the reference's state.rs.

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	flezha "github.com/flezha/flezha_go_sdk"
)

func stateFixtures() []flezha.Relay {
	return []flezha.Relay{
		fixtureRelay{flezha.RelaySpec{FactoryID: "bench_keyed_nopersist", Durable: false}, keyedUpsertChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "store_input"}, storeInputChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "stateful_counter"}, statefulCounterChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "persistence_off_counter", Durable: false}, persistenceOffCounterChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "sleeping_counter", Durable: fixtureDurable()}, sleepingCounterChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "coalescing_counter"}, coalescingCounterChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "resume_probe"}, resumeProbeChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "bench_keyed"}, keyedUpsertChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "schema_evolution_v1"}, schemaEvolutionV1Channels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "schema_evolution_v2"}, schemaEvolutionV2Channels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "tier_store", Run: effectivelyOnceState("effectively_once")}, tierStoreChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "remote_tier_store", Run: effectivelyOnceState("effectively_once"), Durable: "remote"}, tierStoreChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "tier_chain", Run: effectivelyOnceState("effectively_once")}, tierChainChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "checkpoint_store", Run: effectivelyOnceState(ackState("checkpoint"))}, checkpointStoreChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "checkpoint_batched_store", Run: effectivelyOnceState(ackState(map[string]any{
			"checkpoint": map[string]any{"outputs": "at_checkpoint", "max_delay_ms": 1_500},
		}))}, checkpointStoreChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "bench_checkpoint", Run: effectivelyOnceState(ackState(benchCheckpoint()))}, benchCheckpointChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "timer_cap"}, timerCapChannels},
		fixtureRelay{flezha.RelaySpec{FactoryID: "timer_churn"}, timerChurnChannels},
	}
}

// effectivelyOnceState is a flow run under the tier with exactly as its posture.
func effectivelyOnceState(exactly any) map[string]any {
	return map[string]any{"flow": map[string]any{"exactly": exactly}}
}

// ackState is the effectively-once posture acknowledged by ack.
func ackState(ack any) map[string]any {
	return map[string]any{"effectively_once": map[string]any{"ack": ack}}
}

// -- bench_keyed_nopersist / bench_keyed / the tier stores -------------------
//
// tier_store, checkpoint_store and bench_checkpoint are the same keyed upsert
// under three acknowledgement postures; only Run differs.

type upsertValuesState struct {
	Values map[string]string `json:"values"`
}

type upsertChangeState struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (c upsertChangeState) ApplyTo(s *upsertValuesState) {
	if s.Values == nil {
		s.Values = map[string]string{}
	}
	s.Values[c.Key] = c.Value
}

type keyedUpsert struct{}

func (keyedUpsert) InitState(*string) upsertValuesState {
	return upsertValuesState{Values: map[string]string{}}
}

func (keyedUpsert) Changes() flezha.Changes[upsertValuesState] {
	return flezha.Changes[upsertValuesState]{"upsert": upsertChangeState{}}
}

// upsert records and emits one `key:value` input under its natural key; the
// value keeps any later colon.
func (keyedUpsert) upsert(input string, ctx *flezha.Ctx[upsertValuesState, string]) error {
	key, value, _ := strings.Cut(input, ":")
	if err := ctx.Change(upsertChangeState{Key: key, Value: value}); err != nil {
		return err
	}
	return ctx.Emit(key+"="+value, flezha.WithKey(key))
}

type tierStore struct{ keyedUpsert }

func (tierStore) Spec() flezha.Spec { return flezha.Spec{Name: "tier_store_tasklet"} }

func (t tierStore) Run(input string, ctx *flezha.Ctx[upsertValuesState, string]) error {
	// Dead-letter e2e: a "poison" value fails every attempt, exhausting the
	// retry budget so the tier-on atomic park advances the cell past it.
	if _, value, _ := strings.Cut(input, ":"); value == "poison" {
		return errors.New("poison value")
	}
	return t.upsert(input, ctx)
}

// checkpointStore is tier_store's twin, whose interval closes on a `commit:` input.
type checkpointStore struct{ keyedUpsert }

func (checkpointStore) Spec() flezha.Spec { return flezha.Spec{Name: "checkpoint_store_tasklet"} }

func (t checkpointStore) Run(input string, ctx *flezha.Ctx[upsertValuesState, string]) error {
	if prefix(input) == "commit" {
		ctx.Checkpoint()
		return nil
	}
	return t.upsert(input, ctx)
}

// benchCheckpointTasklet is checkpoint_store driven by a declared cadence
// rather than an input.
type benchCheckpointTasklet struct{ keyedUpsert }

func (benchCheckpointTasklet) Spec() flezha.Spec {
	return flezha.Spec{Name: "bench_checkpoint_tasklet"}
}

func (t benchCheckpointTasklet) Run(input string, ctx *flezha.Ctx[upsertValuesState, string]) error {
	return t.upsert(input, ctx)
}

// keyedUpsertChannels is the same keyed upsert under both durability postures,
// so the round-trip bench can isolate the cost of per-Cell persistence.
func keyedUpsertChannels(c *flezha.ChannelBuilder) {
	in := textIn(c, "input").OnFull(flezha.DropNothing)
	flezha.Into(in, tierStore{}).Keyed().Output("sink")
}

// tierStoreChannels: a 300 ms backoff base keeps the min-watermark freeze
// observable in the DLQ e2e, and DropNothing keeps a shed offset from holding
// the min-watermark while the durable input log behind it grows.
func tierStoreChannels(c *flezha.ChannelBuilder) {
	in := textIn(c, "input").OnFull(flezha.DropNothing).RetryBackoffMs(300)
	flezha.Into(in, tierStore{}).Keyed().Output("sink")
}

func checkpointStoreChannels(c *flezha.ChannelBuilder) {
	flezha.Into(textIn(c, "input"), checkpointStore{}).Keyed().Output("sink")
}

func benchCheckpointChannels(c *flezha.ChannelBuilder) {
	in := textIn(c, "input").OnFull(flezha.DropNothing)
	flezha.Into(in, benchCheckpointTasklet{}).Keyed().Output("sink")
}

// -- tier_chain --------------------------------------------------------------
//
// An effectively-once stream of two tasklets. tier_pass counts what it
// forwards and passes each `key:value` on; tier_tally counts each key's inputs
// and emits `key=count`. Until the release env is set the tally fails the
// value `hold` and keeps retrying it, so a runner killed there has made the
// forward's record durable and not the tally's.

type counterValueState struct {
	Value int64 `json:"value"`
}

type counterDeltaState struct {
	Delta int64 `json:"delta"`
}

func (c counterDeltaState) ApplyTo(s *counterValueState) { s.Value += c.Delta }

type counting struct{}

func (counting) InitState(*string) counterValueState { return counterValueState{} }

func (counting) Changes() flezha.Changes[counterValueState] {
	return flezha.Changes[counterValueState]{"delta": counterDeltaState{}}
}

type tierPass struct{ counting }

func (tierPass) Spec() flezha.Spec { return flezha.Spec{Name: "tier_pass"} }

func (tierPass) Run(input string, ctx *flezha.Ctx[counterValueState, string]) error {
	if err := ctx.Change(counterDeltaState{Delta: 1}); err != nil {
		return err
	}
	return ctx.Emit(input)
}

type tierTally struct{ counting }

func (tierTally) Spec() flezha.Spec { return flezha.Spec{Name: "tier_tally"} }

func (tierTally) Run(input string, ctx *flezha.Ctx[counterValueState, string]) error {
	key, value, _ := strings.Cut(input, ":")
	if value == "hold" && !stepsReleased() {
		return errors.New("held until released")
	}
	if err := ctx.Change(counterDeltaState{Delta: 1}); err != nil {
		return err
	}
	return ctx.Emit(fmt.Sprintf("%s=%d", key, ctx.State().Value), flezha.WithKey(key))
}

func tierChainChannels(c *flezha.ChannelBuilder) {
	in := textIn(c, "input").OnFull(flezha.DropNothing)
	passed := flezha.Into(in, tierPass{}).KeyBy(prefix).RetryMax(1_000)
	flezha.Into(passed, tierTally{}).Keyed().Output("sink")
}

// -- store_input -------------------------------------------------------------

// storeInputValuesState is the durable-state reference: every input is
// appended through a change. binary_inputs stays empty, as in every twin.
type storeInputValuesState struct {
	BinaryInputs []any    `json:"binary_inputs"`
	TextInputs   []string `json:"text_inputs"`
}

type storeInputPushState struct {
	PushText string `json:"push_text"`
}

func (c storeInputPushState) ApplyTo(s *storeInputValuesState) {
	s.TextInputs = append(s.TextInputs, c.PushText)
}

type storeInput struct{}

func (storeInput) Spec() flezha.Spec { return flezha.Spec{Name: "store_input_tasklet"} }

func (storeInput) InitState(*string) storeInputValuesState {
	return storeInputValuesState{BinaryInputs: []any{}, TextInputs: []string{}}
}

func (storeInput) Changes() flezha.Changes[storeInputValuesState] {
	return flezha.Changes[storeInputValuesState]{"push_text": storeInputPushState{}}
}

func (storeInput) Run(input string, ctx *flezha.Ctx[storeInputValuesState, string]) error {
	if err := ctx.Change(storeInputPushState{PushText: input}); err != nil {
		return err
	}
	return ctx.Emit(input)
}

func storeInputChannels(c *flezha.ChannelBuilder) {
	flezha.Into(textIn(c, "input"), storeInput{}).Output("sink")
}

// -- stateful_counter / persistence_off_counter ------------------------------

type counterEntry struct{}

func (counterEntry) Spec() flezha.Spec { return flezha.Spec{Name: "counter_entry_tasklet"} }

func (counterEntry) Run(input string, ctx *flezha.Ctx[flezha.NoState, int64]) error {
	return ctx.Emit(wireInt(input))
}

type counter struct{ counting }

func (counter) Spec() flezha.Spec { return flezha.Spec{Name: "counter_tasklet"} }

func (counter) Run(input int64, ctx *flezha.Ctx[counterValueState, flezha.NoOutput]) error {
	return ctx.Change(counterDeltaState{Delta: input})
}

func statefulCounterChannels(c *flezha.ChannelBuilder) {
	ticks := flezha.Into(textIn(c, "input"), counterEntry{}).Named("ticks")
	flezha.Into(ticks, counter{})
}

// persistenceOffCounterChannels is the same topology under the reference's
// node names: persistence off is a relay setting, not a different computation.
func persistenceOffCounterChannels(c *flezha.ChannelBuilder) {
	ticks := flezha.IntoNamed(textIn(c, "input"), counterEntry{}, "persistence_off_entry_tasklet").Named("ticks")
	flezha.IntoNamed(ticks, counter{}, "persistence_off_counter_tasklet")
}

// -- sleeping_counter --------------------------------------------------------
//
// A flow relay that sleeps when idle: `key:delta` inputs fold into one running
// total per key, and every input emits `key=total` on the `totals` output. The
// sleep e2e reads the total after a wake to prove the Cell came back with the
// state it slept with.

type sleepingCounter struct{ counting }

func (sleepingCounter) Spec() flezha.Spec { return flezha.Spec{Name: "sleeping_counter_tasklet"} }

func (sleepingCounter) Run(input string, ctx *flezha.Ctx[counterValueState, string]) error {
	key, delta, _ := strings.Cut(input, ":")
	if err := ctx.Change(counterDeltaState{Delta: wireInt(delta)}); err != nil {
		return err
	}
	return ctx.Emit(fmt.Sprintf("%s=%d", key, ctx.State().Value))
}

func sleepingCounterChannels(c *flezha.ChannelBuilder) {
	flezha.Into(textIn(c, "input").KeyBy(prefix), sleepingCounter{}).Output("totals")
}

// -- coalescing_counter ------------------------------------------------------

// tickInputState is one tick, on the edge and as the change it records.
type tickInputState struct {
	Interval int64 `json:"interval"`
	Ts       int64 `json:"ts"`
	Delta    int64 `json:"delta"`
}

type tickLastState struct {
	Last int64 `json:"last"`
}

func (c tickInputState) ApplyTo(s *tickLastState) { s.Last = c.Delta }

// tickEntry reads `<interval>:<ts>:<delta>`; a missing or unparsable part reads 0.
type tickEntry struct{}

func (tickEntry) Spec() flezha.Spec { return flezha.Spec{Name: "tick_entry_tasklet"} }

func (tickEntry) Run(input string, ctx *flezha.Ctx[flezha.NoState, tickInputState]) error {
	parts := append(strings.SplitN(input, ":", 3), "", "")
	return ctx.Emit(tickInputState{Interval: wireInt(parts[0]), Ts: wireInt(parts[1]), Delta: wireInt(parts[2])})
}

// tick: the reference coalesces this change on (interval, ts); the SDK has no
// coalesce-key seam, so it ships uncoalesced. The fold is idempotent per
// (interval, ts) either way, so replay produces the same state.
type tick struct{}

func (tick) Spec() flezha.Spec { return flezha.Spec{Name: "tick_tasklet"} }

func (tick) InitState(*string) tickLastState { return tickLastState{} }

func (tick) Changes() flezha.Changes[tickLastState] {
	return flezha.Changes[tickLastState]{"tick": tickInputState{}}
}

func (tick) Run(input tickInputState, ctx *flezha.Ctx[tickLastState, flezha.NoOutput]) error {
	return ctx.Change(input)
}

func coalescingCounterChannels(c *flezha.ChannelBuilder) {
	ticks := flezha.Into(textIn(c, "input"), tickEntry{}).Named("ticks")
	flezha.Into(ticks, tick{})
}

// -- resume_probe ------------------------------------------------------------

type totalCountState struct {
	Total int64 `json:"total"`
}

type totalAddState struct {
	Add int64 `json:"add"`
}

func (c totalAddState) ApplyTo(s *totalCountState) { s.Total += c.Add }

type totalingState struct{}

func (totalingState) InitState(*string) totalCountState { return totalCountState{} }

func (totalingState) Changes() flezha.Changes[totalCountState] {
	return flezha.Changes[totalCountState]{"add": totalAddState{}}
}

// resumeProbe emits the running total, so a resume is observable on the wire;
// change replay rebuilds the total on restore.
type resumeProbe struct{ totalingState }

func (resumeProbe) Spec() flezha.Spec { return flezha.Spec{Name: "resume_probe_tasklet"} }

func (resumeProbe) Run(input string, ctx *flezha.Ctx[totalCountState, string]) error {
	delta := wireInt(input)
	total := ctx.State().Total + delta
	if err := ctx.Change(totalAddState{Add: delta}); err != nil {
		return err
	}
	return ctx.Emit(strconv.FormatInt(total, 10))
}

func resumeProbeChannels(c *flezha.ChannelBuilder) {
	flezha.Into(textIn(c, "input"), resumeProbe{}).Output("total")
}

// -- schema_evolution_v1 / schema_evolution_v2 -------------------------------
//
// The same tasklet either side of a deploy that edits its own state type. Both
// halves declare the SAME tasklet name, so a relay started under v1 and
// restarted under v2 restores from the very WAL v1 wrote.
//
// v1: {total}. v2: {total, currency} at schema version 2, reading down to 1
// through a declared migration. Output is `total:currency` so the e2e can read
// the migrated field off the wire.

type evolvingCartV1 struct{ totalingState }

func (evolvingCartV1) Spec() flezha.Spec { return flezha.Spec{Name: "evolving_cart"} }

func (evolvingCartV1) Run(input string, ctx *flezha.Ctx[totalCountState, string]) error {
	delta := wireInt(input)
	total := ctx.State().Total + delta
	if err := ctx.Change(totalAddState{Add: delta}); err != nil {
		return err
	}
	return ctx.Emit(strconv.FormatInt(total, 10) + ":")
}

type cartV2State struct {
	Total    int64  `json:"total"`
	Currency string `json:"currency"`
}

type cartV2AddState struct {
	Add  int64  `json:"add"`
	Note string `json:"note"`
}

func (c cartV2AddState) ApplyTo(s *cartV2State) { s.Total += c.Add }

type evolvingCartV2 struct{}

func (evolvingCartV2) Spec() flezha.Spec {
	return flezha.Spec{Name: "evolving_cart", SchemaVersion: 2, MinReadableSchemaVersion: 1}
}

func (evolvingCartV2) InitState(*string) cartV2State { return cartV2State{} }

func (evolvingCartV2) Changes() flezha.Changes[cartV2State] {
	return flezha.Changes[cartV2State]{"add": cartV2AddState{}}
}

func (evolvingCartV2) MigrateState(fromVersion uint32, bytes []byte) (cartV2State, error) {
	if fromVersion != 1 {
		return cartV2State{}, fmt.Errorf("no migration from %d", fromVersion)
	}
	var old totalCountState
	if err := json.Unmarshal(bytes, &old); err != nil {
		return cartV2State{}, err
	}
	return cartV2State{Total: old.Total, Currency: "EUR"}, nil
}

// MigrateChange reads a change v1 recorded under the name "add".
func (evolvingCartV2) MigrateChange(fromVersion uint32, name string, value []byte) (flezha.Change[cartV2State], error) {
	if fromVersion != 1 || name != "add" {
		return nil, fmt.Errorf("no migration of change %q from %d", name, fromVersion)
	}
	var old totalAddState
	if err := json.Unmarshal(value, &old); err != nil {
		return nil, err
	}
	return cartV2AddState{Add: old.Add, Note: "migrated"}, nil
}

func (evolvingCartV2) Run(input string, ctx *flezha.Ctx[cartV2State, string]) error {
	delta := wireInt(input)
	state := ctx.State()
	currency := state.Currency
	if currency == "" {
		currency = "EUR"
	}
	if err := ctx.Change(cartV2AddState{Add: delta, Note: "live"}); err != nil {
		return err
	}
	return ctx.Emit(fmt.Sprintf("%d:%s", state.Total+delta, currency))
}

func schemaEvolutionV1Channels(c *flezha.ChannelBuilder) {
	flezha.Into(textIn(c, "input"), evolvingCartV1{}).Output("total")
}

func schemaEvolutionV2Channels(c *flezha.ChannelBuilder) {
	flezha.Into(textIn(c, "input"), evolvingCartV2{}).Output("total")
}

// -- timer_cap ---------------------------------------------------------------
//
// The core's refusal of a Cell's timers when the item settles: past the cap of
// two pending timers, or an event-time timer under a tasklet whose clock is not
// event time. `event` asks for the event-time timer, a number `n` for a
// processing-time one `n` hours ahead; the change follows.

type touchedListState struct {
	Touched []string `json:"touched"`
}

type touchedChangeState struct {
	Touched string `json:"touched"`
}

func (c touchedChangeState) ApplyTo(s *touchedListState) { s.Touched = append(s.Touched, c.Touched) }

type timerCap struct{}

func (timerCap) Spec() flezha.Spec {
	return flezha.Spec{Name: "timer_cap_tasklet", MaxTimersPerCell: 2}
}

func (timerCap) InitState(*string) touchedListState { return touchedListState{Touched: []string{}} }

func (timerCap) Changes() flezha.Changes[touchedListState] {
	return flezha.Changes[touchedListState]{"touched": touchedChangeState{}}
}

func (timerCap) Run(input string, ctx *flezha.Ctx[touchedListState, string]) error {
	var err error
	if input == "event" {
		_, err = ctx.TimerAtEventTime(1)
	} else {
		_, err = ctx.TimerAfter(uint64(max(wireInt(input), 0)) * 3_600_000)
	}
	if err != nil {
		return err
	}
	return ctx.Change(touchedChangeState{Touched: input})
}

func timerCapChannels(c *flezha.ChannelBuilder) {
	flezha.Into(textIn(c, "input"), timerCap{}).Output("kafka")
}

// -- timer_churn -------------------------------------------------------------
//
// The bench factory for `mix bench --shape timer`: each input arms a 1 ms
// processing-time timer and cancels the one the previous input armed; the
// input is echoed, and a firing emits `fired:<at>`.

type churnLastState struct {
	Last uint64 `json:"last"`
}

type churnArmedState struct {
	Armed uint64 `json:"armed"`
}

func (c churnArmedState) ApplyTo(s *churnLastState) { s.Last = c.Armed }

type timerChurn struct{}

func (timerChurn) Spec() flezha.Spec { return flezha.Spec{Name: "timer_churn_tasklet"} }

func (timerChurn) InitState(*string) churnLastState { return churnLastState{} }

func (timerChurn) Changes() flezha.Changes[churnLastState] {
	return flezha.Changes[churnLastState]{"armed": churnArmedState{}}
}

func (timerChurn) Run(input string, ctx *flezha.Ctx[churnLastState, string]) error {
	at, err := ctx.TimerAfter(1)
	if err != nil {
		return err
	}
	if last := ctx.State().Last; last != 0 && last != at {
		if err := ctx.CancelTimer(flezha.ProcessingTime, last); err != nil {
			return err
		}
	}
	if err := ctx.Change(churnArmedState{Armed: at}); err != nil {
		return err
	}
	return ctx.Emit(input)
}

func (timerChurn) OnTimer(t flezha.TimerFiring, ctx *flezha.Ctx[churnLastState, string]) error {
	return ctx.Emit(fmt.Sprintf("fired:%d", t.ScheduledAt))
}

func timerChurnChannels(c *flezha.ChannelBuilder) {
	flezha.Into(textIn(c, "input"), timerChurn{}).Output("echo")
}
