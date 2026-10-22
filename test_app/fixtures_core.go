// Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
// under the same factory ids, tasklet names, channel names and output bytes.
// Entry payloads and plugin frames are UTF-8 text on the wire, as for the
// reference; an inter-tasklet hop between two Go tasklets carries JSON, which
// no one else reads.

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	flezha "github.com/flezha/flezha_go_sdk"
)

// passCore answers its input unchanged, under the name it is given; "" for a
// short form, which names its tasklet after the factory.
type passCore struct{ name string }

func (t passCore) Spec() flezha.Spec { return flezha.Spec{Name: t.name} }

func (passCore) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in)
}

// prefixedCore emits its input behind a fixed prefix.
type prefixedCore struct{ name, prefix string }

func (t prefixedCore) Spec() flezha.Spec { return flezha.Spec{Name: t.name} }

func (t prefixedCore) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(t.prefix + in)
}

// seenCore is the state of every fixture that keeps each input it is sent.
type seenCore struct {
	Seen []string `json:"seen"`
}

type pushCore struct {
	Push string `json:"push"`
}

// Clipped, so an append never writes into an array an earlier state shares.
func (c pushCore) ApplyTo(s *seenCore) { s.Seen = append(slices.Clip(s.Seen), c.Push) }

// recorderCore keeps each input in state and answers it unchanged.
type recorderCore struct{ spec flezha.Spec }

func (t recorderCore) Spec() flezha.Spec { return t.spec }

func (recorderCore) InitState(*string) seenCore { return seenCore{Seen: []string{}} }

func (recorderCore) Changes() flezha.Changes[seenCore] {
	return flezha.Changes[seenCore]{"push": pushCore{}}
}

func (recorderCore) Run(in string, ctx *flezha.Ctx[seenCore, string]) error {
	if err := ctx.Change(pushCore{Push: in}); err != nil {
		return err
	}
	return ctx.Emit(in)
}

// -- do_nothing / bench_echo -------------------------------------------------

var doNothingCore = passCore{name: "do_nothing_tasklet"}

func doNothingRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "do_nothing"}, func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "input"), doNothingCore).Output("echo")
	}}
}

// Echo with a declared DropNothing on its entry mailbox. `do_nothing` is the
// merge-blocking bench factory and must not change; this keeps the round-trip
// bench's back-pressure posture independent of it.
func benchEchoRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "bench_echo"}, func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "input").OnFull(flezha.DropNothing), doNothingCore).Output("echo")
	}}
}

// -- compare_filter ----------------------------------------------------------

type compareStateCore struct {
	Comparable string `json:"comparable"`
	Inverted   bool   `json:"inverted"`
}

// compareFilterCore's state is seeded by the controller's init values and then
// only read: there is nothing to journal, so it declares no changes.
type compareFilterCore struct{}

func (compareFilterCore) Spec() flezha.Spec { return flezha.Spec{Name: "compare_filter_tasklet"} }

func (compareFilterCore) InitState(*string) compareStateCore {
	return compareStateCore{Comparable: "default"}
}

func (compareFilterCore) Run(in string, ctx *flezha.Ctx[compareStateCore, string]) error {
	if ctx.State().Comparable == in {
		return ctx.Emit(in)
	}
	return nil
}

func compareFilterRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "compare_filter"}, func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "input"), compareFilterCore{}).Output("sink")
	}}
}

// -- static_cell / sugar_static_cell / mixed_relay ---------------------------

// staticEntryCore is a recorder on the static cells a..d, routed by the
// input's first character.
type staticEntryCore struct{ recorderCore }

func newStaticEntryCore(name string) staticEntryCore {
	return staticEntryCore{recorderCore{flezha.Spec{Name: name, CellKeys: []string{"a", "b", "c", "d"}}}}
}

func (staticEntryCore) CellsFor(in string) []string {
	if in != "" && in[0] >= 'a' && in[0] <= 'd' {
		return []string{in[:1]}
	}
	return []string{"d"}
}

func staticCellRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "static_cell"}, func(c *flezha.ChannelBuilder) {
		hop := flezha.Into(textIn(c, "input"), newStaticEntryCore("static_entry_tasklet")).Named("hop")
		flezha.Into(hop, passCore{name: "static_downstream_tasklet"}).Output("kafka")
	}}
}

// The reference pipeline sugar leaves the hop unnamed and lets the output name
// the producing channel.
func sugarStaticCellRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "sugar_static_cell"}, func(c *flezha.ChannelBuilder) {
		entry := flezha.Into(textIn(c, "input"), newStaticEntryCore("macro_static_entry_tasklet"))
		flezha.Into(entry, passCore{name: "macro_static_downstream_tasklet"}).Output("test_kafka")
	}}
}

func mixedRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "mixed_relay"}, func(c *flezha.ChannelBuilder) {
		hop := flezha.Into(textIn(c, "input"), passCore{name: "mixed_macro_entry_tasklet"}).Named("hop")
		flezha.Into(hop, passCore{name: "mixed_hand_downstream"}).Output("kafka")
	}}
}

// -- the short forms ---------------------------------------------------------

func sugarDoNothingRelay() flezha.Relay {
	return flezha.ShortTasklet("sugar_do_nothing", passCore{}, flezha.ShortOptions{
		Codec: flezha.Text, Output: "sugar_do_nothing_echo",
	})
}

type textInputsCore struct {
	TextInputs []string `json:"text_inputs"`
}

type textPushCore struct {
	Push string `json:"push"`
}

func (c textPushCore) ApplyTo(s *textInputsCore) {
	s.TextInputs = append(slices.Clip(s.TextInputs), c.Push)
}

type sugarStoreInputCore struct{}

func (sugarStoreInputCore) Spec() flezha.Spec { return flezha.Spec{} }

func (sugarStoreInputCore) InitState(*string) textInputsCore {
	return textInputsCore{TextInputs: []string{}}
}

func (sugarStoreInputCore) Changes() flezha.Changes[textInputsCore] {
	return flezha.Changes[textInputsCore]{"push": textPushCore{}}
}

func (sugarStoreInputCore) Run(in string, ctx *flezha.Ctx[textInputsCore, string]) error {
	if err := ctx.Change(textPushCore{Push: in}); err != nil {
		return err
	}
	return ctx.Emit(in)
}

func sugarStoreInputRelay() flezha.Relay {
	return flezha.ShortTasklet("sugar_store_input", sugarStoreInputCore{}, flezha.ShortOptions{
		Codec: flezha.Text, Output: "sugar_store_sink",
	})
}

func sugarNamedInputRelay() flezha.Relay {
	return flezha.ShortTasklet("sugar_named_input", passCore{}, flezha.ShortOptions{
		Input: "prices", Codec: flezha.Text, Output: "sugar_named_input_sink",
	})
}

func sugarStepsRelay() flezha.Relay {
	return flezha.ShortTasklet("sugar_steps", passCore{}, flezha.ShortOptions{
		Input: "order", Codec: flezha.Text, Output: "sugar_steps_placed",
		Run: "steps", Durable: fixtureDurable(),
	})
}

// The loop mode, the one class-level setting a short form keeps.
func sugarStepsPerInputRelay() flezha.Relay {
	return flezha.ShortTasklet("sugar_steps_per_input", passCore{}, flezha.ShortOptions{
		Codec: flezha.Text, Output: "sugar_steps_per_input_out",
		Run:     map[string]any{"steps": map[string]any{"each": "per_input"}},
		Durable: fixtureDurable(),
	})
}

// sugarStateOnlyCore only keeps what it is sent, and answers nothing.
type sugarStateOnlyCore struct{}

func (sugarStateOnlyCore) Spec() flezha.Spec { return flezha.Spec{} }

func (sugarStateOnlyCore) InitState(*string) seenCore { return seenCore{Seen: []string{}} }

func (sugarStateOnlyCore) Changes() flezha.Changes[seenCore] {
	return flezha.Changes[seenCore]{"push": pushCore{}}
}

func (sugarStateOnlyCore) Run(in string, ctx *flezha.Ctx[seenCore, flezha.NoOutput]) error {
	return ctx.Change(pushCore{Push: in})
}

func sugarStateOnlyRelay() flezha.Relay {
	return flezha.ShortTasklet("sugar_state_only", sugarStateOnlyCore{}, flezha.ShortOptions{
		Input: "ticks", Codec: flezha.Text,
	})
}

// -- hybrid_channel ----------------------------------------------------------
//
// One emit reaching two destinations: an inter-tasklet consumer and a plugin
// output with its own encoder. The encoder refuses a sentinel value.

const hybridSentinelCore = 0xDEADBEEF

type hybridProducerCore struct{}

func (hybridProducerCore) Spec() flezha.Spec { return flezha.Spec{Name: "hybrid_producer_tasklet"} }

func (hybridProducerCore) Run(in string, ctx *flezha.Ctx[flezha.NoState, uint32]) error {
	value, err := strconv.ParseUint(in, 10, 32)
	if err != nil {
		value = 0
	}
	// A refused plugin encoding fails this emit alone; the item completes.
	if err := ctx.Emit(uint32(value)); err != nil {
		fmt.Fprintf(os.Stderr, "hybrid_producer_tasklet: serializer refused an emit: %v\n", err)
	}
	return nil
}

type hybridConsumerCore struct{}

func (hybridConsumerCore) Spec() flezha.Spec { return flezha.Spec{Name: "hybrid_consumer_tasklet"} }

func (hybridConsumerCore) Run(in uint32, _ *flezha.Ctx[flezha.NoState, flezha.NoOutput]) error {
	fmt.Printf("hybrid_consumer_tasklet: received %d\n", in)
	return nil
}

func hybridChannelRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "hybrid_channel"}, func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "input"), hybridProducerCore{}).Branch(
			// The consumer edge first, so a refused encoding leaves the
			// in-process fan-out already recorded.
			func(edge flezha.Stream[uint32]) {
				flezha.Into(edge.Named("hybrid_indicator"), hybridConsumerCore{})
			},
			func(edge flezha.Stream[uint32]) {
				edge.OutputEncoded("", func(v uint32) ([]byte, error) {
					if v == hybridSentinelCore {
						return nil, errors.New("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF")
					}
					return binary.BigEndian.AppendUint32(nil, v), nil
				})
			},
		)
	}}
}

// -- merged_input ------------------------------------------------------------
//
// Two entry channels merged into one tasklet. Each source's mapping tags the
// value with the channel it came from, the merge keys on the symbol the mapping
// read, and the emitted line is `<source>:<payload>`.

type mergedCore struct {
	Source  string `json:"source"`
	Symbol  string `json:"symbol"`
	Payload string `json:"payload"`
}

type mergeLabelCore struct{}

func (mergeLabelCore) Spec() flezha.Spec { return flezha.Spec{Name: "merge_label_tasklet"} }

func (mergeLabelCore) Run(in mergedCore, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in.Source + ":" + in.Payload)
}

func taggedCore(source string) func(string) mergedCore {
	return func(payload string) mergedCore {
		return mergedCore{Source: source, Symbol: prefix(payload), Payload: payload}
	}
}

func mergedInputRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "merged_input"}, func(c *flezha.ChannelBuilder) {
		ticks := textIn(c, "ticks")
		quotes := textIn(c, "quotes")
		m := flezha.NewMerge[mergedCore](c)
		flezha.From(m, ticks, taggedCore("ticks"))
		flezha.From(m, quotes, taggedCore("quotes"))
		m.KeyBy(func(v mergedCore) string { return v.Symbol })
		flezha.MergeInto(m, mergeLabelCore{}).Keyed().Output("merged")
	}}
}

// -- transform_chain ---------------------------------------------------------
//
// Every value transform on one edge: `drop` is filtered out, a comma list fans
// out into one value per element, each is upper-cased and keyed on its prefix.

func transformChainRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "transform_chain"}, func(c *flezha.ChannelBuilder) {
		trades := textIn(c, "trades").Filter(func(trade string) bool { return trade != "drop" })
		upper := flezha.Map(flezha.FlatMap(trades, func(trade string) []string {
			return strings.Split(trade, ",")
		}), strings.ToUpper)
		flezha.Into(upper.KeyBy(prefix), passCore{name: "transform_label_tasklet"}).Keyed().Output("transformed")
	}}
}

// -- cell_stress -------------------------------------------------------------

var stressCellsCore = []string{"p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"}

// stressCore passes its input through on the eight static cells: to every
// one of them when fanning out, else to p0.
type stressCore struct {
	name   string
	fanOut bool
}

func (t stressCore) Spec() flezha.Spec { return flezha.Spec{Name: t.name, CellKeys: stressCellsCore} }

func (t stressCore) CellsFor(string) []string {
	if t.fanOut {
		return stressCellsCore
	}
	return []string{"p0"}
}

func (stressCore) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in)
}

func cellStressRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "cell_stress"}, func(c *flezha.ChannelBuilder) {
		ab := flezha.Into(textIn(c, "input"), stressCore{name: "stress_a", fanOut: true}).Named("a_to_b")
		bc := flezha.Into(ab, stressCore{name: "stress_b"}).Named("b_to_c")
		cd := flezha.Into(bc, stressCore{name: "stress_c"}).Named("c_to_d")
		flezha.Into(cd, stressCore{name: "stress_d"}).Output("echo")
	}}
}

// -- candle ------------------------------------------------------------------
//
// Late-input correction is about fold ORDER. Send 100, 300, then the late 200:
// with correction the engine rewinds behind 200, replays the retained input
// log sorted by event time, and the fold order becomes [100, 200, 300].

type candleStateCore struct {
	Folded []uint64 `json:"folded"`
}

type foldCore struct {
	Fold uint64 `json:"fold"`
}

func (c foldCore) ApplyTo(s *candleStateCore) { s.Folded = append(slices.Clip(s.Folded), c.Fold) }

type candleCore struct{}

func (candleCore) Spec() flezha.Spec { return flezha.Spec{Name: "candle_tasklet"} }

func (candleCore) InitState(*string) candleStateCore { return candleStateCore{Folded: []uint64{}} }

func (candleCore) Changes() flezha.Changes[candleStateCore] {
	return flezha.Changes[candleStateCore]{"fold": foldCore{}}
}

func (candleCore) EventTime(in string) uint64 {
	at, err := strconv.ParseUint(prefix(in), 10, 64)
	if err != nil {
		return 0
	}
	return at
}

func (candleCore) Run(in string, ctx *flezha.Ctx[candleStateCore, string]) error {
	// Now() IS the extracted event time under an event-time clock.
	eventTime := ctx.Now()
	if err := ctx.Change(foldCore{Fold: eventTime}); err != nil {
		return err
	}
	// One row per candle bucket, so a corrected replay overwrites by key
	// rather than appending a second row.
	bucket := strconv.FormatUint(eventTime/1000, 10)
	return ctx.Emit(bucket+"="+in, flezha.WithKey("candle/"+bucket))
}

func candleRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{
		FactoryID: "candle",
		Run: map[string]any{"flow": map[string]any{"exactly": map[string]any{
			"effectively_once": map[string]any{"correction_ms": 600_000},
		}}},
	}, func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "input"), candleCore{}).Keyed().Output("sink")
	}}
}

// -- session_timeout ---------------------------------------------------------
//
// Each input pushes the idle deadline out, and the firing closes the session.
// Single-cell, so the e2e's crash-resume half exercises a durable cell.

type sessionStateCore struct {
	Touched  []string `json:"touched"`
	ClosedAt []uint64 `json:"closed_at"`
}

type touchedCore struct {
	Touched string `json:"touched"`
}

func (c touchedCore) ApplyTo(s *sessionStateCore) {
	s.Touched = append(slices.Clip(s.Touched), c.Touched)
}

type closedCore struct {
	Closed uint64 `json:"closed"`
}

func (c closedCore) ApplyTo(s *sessionStateCore) {
	s.ClosedAt = append(slices.Clip(s.ClosedAt), c.Closed)
}

type sessionTimeoutCore struct{}

func (sessionTimeoutCore) Spec() flezha.Spec { return flezha.Spec{Name: "session_timeout_tasklet"} }

func (sessionTimeoutCore) InitState(*string) sessionStateCore {
	return sessionStateCore{Touched: []string{}, ClosedAt: []uint64{}}
}

func (sessionTimeoutCore) Changes() flezha.Changes[sessionStateCore] {
	return flezha.Changes[sessionStateCore]{"touched": touchedCore{}, "closed": closedCore{}}
}

func (sessionTimeoutCore) Run(in string, ctx *flezha.Ctx[sessionStateCore, string]) error {
	if err := ctx.Change(touchedCore{Touched: in}); err != nil {
		return err
	}
	timeout := wireInt(os.Getenv("TIMER_TEST_TIMEOUT_MS"))
	if timeout <= 0 {
		timeout = 1000
	}
	// A processing-time timer: nothing arriving would advance an event-time clock.
	_, err := ctx.TimerAfter(uint64(timeout))
	return err
}

func (sessionTimeoutCore) OnTimer(t flezha.TimerFiring, ctx *flezha.Ctx[sessionStateCore, string]) error {
	if err := ctx.Change(closedCore{Closed: t.ScheduledAt}); err != nil {
		return err
	}
	return ctx.Emit("closed@" + strconv.FormatUint(t.ScheduledAt, 10))
}

func sessionTimeoutRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "session_timeout"}, func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "input"), sessionTimeoutCore{}).Output("kafka")
	}}
}

// -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} ------------------

type processedStateCore struct {
	Processed int `json:"processed"`
}

type processedCore struct {
	Processed bool `json:"processed"`
}

func (processedCore) ApplyTo(s *processedStateCore) { s.Processed++ }

// boundedMailboxCore counts what it processes, behind a mailbox of four.
type boundedMailboxCore struct{ name string }

func (t boundedMailboxCore) Spec() flezha.Spec {
	capacity := 4
	return flezha.Spec{Name: t.name, MailboxCapacity: &capacity}
}

func (boundedMailboxCore) Changes() flezha.Changes[processedStateCore] {
	return flezha.Changes[processedStateCore]{"processed": processedCore{}}
}

func (boundedMailboxCore) Run(in string, ctx *flezha.Ctx[processedStateCore, string]) error {
	if err := ctx.Change(processedCore{Processed: true}); err != nil {
		return err
	}
	return ctx.Emit(in)
}

func boundedMailboxRelay(policy flezha.Overflow, mode string) flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "bounded_mailbox_" + mode}, func(c *flezha.ChannelBuilder) {
		in := textIn(c, mode+"_input").OnFull(policy)
		flezha.Into(in, boundedMailboxCore{name: mode + "_test_tasklet"}).Output(mode + "_echo")
	}}
}

// -- partition_pass ----------------------------------------------------------
//
// The partitioned-relays upstream: keyed on the wiring so it can be
// partitioned, and emitting keyed so a partitioned forward target can resolve
// the owning member. A forwarded frame keeps its producing channel's name,
// which selects the receiving relay's entry channel: so the output is `input`
// and the entry `in`.

type partitionPassCore struct{ recorderCore }

func (partitionPassCore) Run(in string, ctx *flezha.Ctx[seenCore, string]) error {
	if err := ctx.Change(pushCore{Push: in}); err != nil {
		return err
	}
	return ctx.Emit(in, flezha.WithKey(prefix(in)))
}

func partitionPassRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "partition_pass"}, func(c *flezha.ChannelBuilder) {
		in := textIn(c, "in").KeyBy(prefix)
		tasklet := partitionPassCore{recorderCore{flezha.Spec{Name: "partition_pass_tasklet"}}}
		flezha.Into(in, tasklet).Keyed().Output("input")
	}}
}

// -- dynamic_cell ------------------------------------------------------------

// dynamicTTLSecondsCore is the eviction TTL `DYN_TEST_TTL_MS` arms, rounded up
// to whole seconds; 0, none, when the variable is unset.
func dynamicTTLSecondsCore() int64 {
	if ms := wireInt(os.Getenv("DYN_TEST_TTL_MS")); ms > 0 {
		return (ms + 999) / 1000
	}
	return 0
}

func dynamicCellRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "dynamic_cell"}, func(c *flezha.ChannelBuilder) {
		// Filtered before keying: a dropped value answers no key, so the core
		// never mints a cell for it.
		in := textIn(c, "input").OnFull(flezha.DropNothing).
			Filter(func(in string) bool { return in != "drop" }).
			KeyBy(prefix)
		tasklet := recorderCore{flezha.Spec{Name: "dynamic_cell_tasklet", IdleTTLSeconds: dynamicTTLSecondsCore()}}
		flezha.Into(in, tasklet).Output("kafka")
	}}
}

// -- call_echo / call_downstream / call_split --------------------------------
//
// Relays an outside caller calls and whose output answers it: from the first
// tasklet, from a downstream one, and from whichever of two filtered paths
// the input took.

func callEchoRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "call_echo"}, func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "in"), passCore{name: "call_echo"}).Output("")
	}}
}

func callDownstreamRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "call_downstream"}, func(c *flezha.ChannelBuilder) {
		parsed := flezha.Into(textIn(c, "in"), passCore{name: "call_parse"})
		flezha.Into(parsed, prefixedCore{name: "call_answer", prefix: "re:"}).Output("")
	}}
}

// An input starting `no` takes the refused path, every other the accepted one.
func callSplitRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "call_split"}, func(c *flezha.ChannelBuilder) {
		admitted := flezha.Into(textIn(c, "in"), passCore{name: "call_admit"})
		refused := func(in string) bool { return strings.HasPrefix(in, "no") }
		accepted := admitted.Filter(func(in string) bool { return !refused(in) })
		flezha.Into(accepted, prefixedCore{name: "call_accept", prefix: "accepted:"}).Output("")
		flezha.Into(admitted.Filter(refused), prefixedCore{name: "call_refuse", prefix: "refused:"}).Output("")
	}}
}

// coreFixtures is every relay fixtures.php declares, in the reference app's
// registration order.
func coreFixtures() []flezha.Relay {
	return []flezha.Relay{
		doNothingRelay(),
		benchEchoRelay(),
		compareFilterRelay(),
		staticCellRelay(),
		sugarStaticCellRelay(),
		mixedRelay(),
		hybridChannelRelay(),
		mergedInputRelay(),
		transformChainRelay(),
		cellStressRelay(),
		candleRelay(),
		sessionTimeoutRelay(),
		boundedMailboxRelay(flezha.DropNothing, "drop_nothing"),
		boundedMailboxRelay(flezha.DropNewest, "drop_newest"),
		boundedMailboxRelay(flezha.DropOldest, "drop_oldest"),
		partitionPassRelay(),
		dynamicCellRelay(),
		spawnRequesterRelay,
		spawnHoldRequesterRelay,
		spawnOnceRequesterRelay,
		callEchoRelay(),
		callDownstreamRelay(),
		callSplitRelay(),
		sugarDoNothingRelay(),
		sugarStoreInputRelay(),
		sugarNamedInputRelay(),
		sugarStepsRelay(),
		sugarStepsPerInputRelay(),
		sugarStateOnlyRelay(),
	}
}
