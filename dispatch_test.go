package flezha

import (
	"errors"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/flezha/flezha_go_sdk/internal/codec"
	"github.com/flezha/flezha_go_sdk/internal/contract"
	"github.com/flezha/flezha_go_sdk/internal/native"
)

type attempts struct{ n *int }

func (attempts) Spec() Spec { return Spec{Name: "attempts"} }

func (a attempts) Run(in string, ctx *Ctx[NoState, string]) error {
	*a.n++
	switch in {
	case "panic-once":
		if *a.n == 1 {
			panic("first attempt")
		}
	case "park":
		return NonRetryable(errors.New("never retry this"))
	}
	return ctx.Emit(in)
}

func (attempts) OnTimer(TimerFiring, *Ctx[NoState, string]) error { return nil }

type flowRelay struct {
	t Tasklet[string, NoState, string]
}

func (flowRelay) Spec() RelaySpec { return RelaySpec{FactoryID: "flow"} }

func (r flowRelay) Channels(c *ChannelBuilder) {
	Into(Input[string](c, "in", InputOptions{Codec: Text}), r.t).Output("out")
}

// recorder is a core that records completions.
type recorder struct {
	fakeCore
	results []codec.Result
}

func (r *recorder) Complete(b []byte) error {
	res, err := codec.DecodeResult(b)
	r.results = append(r.results, res)
	return err
}

func (r *recorder) Abandon(uint64) error { return nil }

// registered builds a relay's class the way RegisterFactory does, judged by the core.
func registered(t *testing.T, relay Relay) (*registry, *registeredClass) {
	t.Helper()
	core, err := native.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer core.Free()
	reg := newRegistry()
	spec, err := declareRelay(core, reg, relay)
	if err != nil {
		t.Fatal(err)
	}
	return reg, reg.get(spec.relay.FactoryID)
}

func work(token uint64, factory string, payloads ...string) codec.Activation {
	a := codec.Activation{Token: token, Relay: factory, Factory: factory, Kind: codec.KindWork, CellIdentity: []byte("id")}
	for _, p := range payloads {
		a.Items = append(a.Items, codec.WorkItem{Channel: 0, Payload: []byte(p), Now: 1000})
	}
	return a
}

func answer(t *testing.T, relay Relay, activations ...codec.Activation) []codec.Result {
	t.Helper()
	reg, _ := registered(t, relay)
	core := &recorder{}
	d := &dispatcher{registry: reg, core: core, log: quietLog()}
	for _, a := range activations {
		d.dispatch(codec.EncodeActivation(a))
	}
	return core.results
}

func TestAPanicIsARetryableFailureAndTheRunnerKeepsServing(t *testing.T) {
	n := 0
	results := answer(t, flowRelay{attempts{&n}}, work(1, "flow", "panic-once"), work(2, "flow", "panic-once"))
	first, second := results[0].Effects[0], results[1].Effects[0]
	if first.Verdict != codec.Retryable || !strings.Contains(first.Message, "panic: first attempt") {
		t.Fatalf("the first attempt answered %+v", first)
	}
	if second.Verdict != codec.Ok || len(second.Outputs) != 1 {
		t.Fatalf("the second attempt answered %+v", second)
	}
}

func TestANonRetryableErrorParksAtOnce(t *testing.T) {
	n := 0
	effect := answer(t, flowRelay{attempts{&n}}, work(1, "flow", "park"))[0].Effects[0]
	if effect.Verdict != codec.NonRetryable || !strings.Contains(effect.Message, "never retry this") {
		t.Fatalf("answered %+v", effect)
	}
}

func TestItemsAfterAFailureAreLeftForTheCore(t *testing.T) {
	n := 0
	effects := answer(t, flowRelay{attempts{&n}}, work(1, "flow", "ok", "park", "never"))[0].Effects
	if len(effects) != 2 || effects[1].Verdict != codec.NonRetryable {
		t.Fatalf("answered %d effects: %+v", len(effects), effects)
	}
}

type jsonRelay struct{}

func (jsonRelay) Spec() RelaySpec { return RelaySpec{FactoryID: "json"} }

func (jsonRelay) Channels(c *ChannelBuilder) {
	Into(Input[int](c, "in"), tallyOf{}).Output("out")
}

type tallyOf struct{}

func (tallyOf) Spec() Spec { return Spec{Name: "tally"} }

func (tallyOf) Changes() Changes[int] { return Changes[int]{"add": add(0)} }

type add int

func (a add) ApplyTo(s *int) { *s += int(a) }

func (tallyOf) Run(in int, ctx *Ctx[int, int]) error {
	if err := ctx.Change(add(in)); err != nil {
		return err
	}
	return ctx.Emit(ctx.State())
}

func TestAnEntryPayloadTheChannelCannotReadIsUnreadable(t *testing.T) {
	effect := answer(t, jsonRelay{}, work(1, "json", "not json"))[0].Effects[0]
	if effect.Verdict != codec.Unreadable {
		t.Fatalf("answered %+v", effect)
	}
}

func TestARestoreFoldRebuildsStateBeforeInputsFlow(t *testing.T) {
	key := "k"
	restore := codec.Activation{Token: 1, Relay: "json", Factory: "json", CellKey: &key, Kind: codec.KindRestoreFold,
		Restore: codec.RestoreFold{Changes: []codec.RestoreBody{{Bytes: []byte(`{"c":"add","v":40}`)}}}}
	input := work(2, "json", "2")
	input.CellKey = &key
	results := answer(t, jsonRelay{}, restore, input)
	if results[0].Outcome != codec.OutcomeRestored {
		t.Fatalf("the fold answered %+v", results[0])
	}
	if out := results[1].Effects[0].Outputs; string(out[0].Payload) != "42" {
		t.Fatalf("the next input saw %s, want the folded 40 plus 2", out[0].Payload)
	}
}

func TestASnapshotAndAnApplyStateCarryTheStateAsJSON(t *testing.T) {
	read := codec.Activation{Token: 3, Relay: "json", Factory: "json", Kind: codec.KindLiveStateRead}
	apply := codec.Activation{Token: 2, Relay: "json", Factory: "json", Kind: codec.KindApplyState, StateJSON: []byte("17")}
	results := answer(t, jsonRelay{}, apply, read)
	if results[0].Outcome != codec.OutcomeStateApplied || string(results[1].Bytes) != "17" {
		t.Fatalf("answered %+v then %+v", results[0], results[1])
	}
}

func TestATimerToATaskletWithoutOnTimerIsRefused(t *testing.T) {
	a := work(1, "json")
	a.Items = []codec.WorkItem{{Timer: true, ScheduledAt: 5}}
	effect := answer(t, jsonRelay{}, a)[0].Effects[0]
	if effect.Verdict != codec.Retryable || !strings.Contains(effect.Message, "runtime.timer_without_on_timer") {
		t.Fatalf("answered %+v", effect)
	}
}

func TestImplementingAHookDeclaresIt(t *testing.T) {
	n := 0
	if !define(attempts{&n}).hasTimer || define(tallyOf{}).hasTimer {
		t.Fatal("OnTimer is declared exactly by the tasklet that implements it")
	}
	if define(tallyOf{}).journals == false || define(attempts{&n}).journals {
		t.Fatal("a tasklet journals exactly when it lists change types")
	}
}

type nameless struct{}

func (nameless) Spec() Spec { return Spec{} }

func (nameless) Run(string, *Ctx[NoState, string]) error { return nil }

func TestATaskletWithoutANameIsRefusedNamingItsType(t *testing.T) {
	r, err := CreateRunner("x", WithLogger(quietLog()))
	if err != nil {
		t.Fatal(err)
	}
	err = r.RegisterFactory(flowRelay{nameless{}})
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != "topology.tasklet_name_missing" || !strings.Contains(refusal.Message, "nameless") {
		t.Fatalf("expected topology.tasklet_name_missing naming the type, got %v", err)
	}
	if r.registry.get("flow") != nil {
		t.Fatal("a refused relay was registered")
	}
}

type cycleRelay struct{}

func (cycleRelay) Spec() RelaySpec { return RelaySpec{FactoryID: "cycle"} }

type hop struct{ name string }

func (h hop) Spec() Spec { return Spec{Name: h.name} }

func (hop) Run(in string, ctx *Ctx[NoState, string]) error { return ctx.Emit(in) }

func (cycleRelay) Channels(c *ChannelBuilder) {
	a := Into(Input[string](c, "in"), hop{"a"})
	b := Into(a, hop2{})
	Into(b, hop{"a"})
}

type hop2 struct{}

func (hop2) Spec() Spec { return Spec{Name: "b"} }

func (hop2) Run(in string, ctx *Ctx[NoState, string]) error { return ctx.Emit(in) }

func TestARefusalTheStaticTypesCannotSeeIsTheCores(t *testing.T) {
	r, err := CreateRunner("x", WithLogger(quietLog()))
	if err != nil {
		t.Fatal(err)
	}
	err = r.RegisterFactory(cycleRelay{})
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != "topology.cycle" {
		t.Fatalf("expected the core's topology.cycle, got %v", err)
	}
}

func TestTheDeclarationKeysAreTheFixtures(t *testing.T) {
	text, err := contract.Read("test/fixtures/host_class_decl_keys.txt")
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, line := range strings.Split(string(text), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			want = append(want, line)
		}
	}
	var got []string
	for section, t := range map[string]reflect.Type{
		"class": reflect.TypeOf(classDecl{}), "channel": reflect.TypeOf(channelDecl{}), "tasklet": reflect.TypeOf(taskletDecl{}),
	} {
		for i := range t.NumField() {
			name, _ := jsonName(t.Field(i))
			got = append(got, section+"."+name)
		}
	}
	sort.Strings(want)
	sort.Strings(got)
	if !slices.Equal(got, want) {
		t.Fatalf("the declaration spells %v\nthe fixture lists %v", got, want)
	}
}

func TestTheStableIDIsTheSharedOne(t *testing.T) {
	identity := []byte{0x04, 0x69, 0x6e, 0x73, 0x74, 0x04, 0x70, 0x72, 0x6f, 0x63, 0x00, 0x02, 0x6b, 0x31}
	for _, c := range []struct {
		identity []byte
		seq      uint64
		ordinal  uint32
		want     string
	}{
		{[]byte("instance/op/key"), 42, 0, "bbf81fd4955afd1724203700a240e822"},
		{[]byte("instance/op/key"), 42, 1, "bbf81fd4965afd1724203700a240e95d"},
		{nil, 0, 0, "15915375576d2f389a938dc3a9940c9d"},
		{identity, 0, 0, "ec2d8145d51f0ef2bc81a81ecef9711f"},
		{identity, 7, 0, "c9cada20361f0ef2bc819fa887d72efe"},
		{identity, 7, 1, "c9cada20371f0ef2bc819fa887d73039"},
		{identity, 12_345, 42, "e24bb5eab21f0ef2bc4a8ded9ae4b92a"},
	} {
		if got := stableID(c.identity, c.seq, c.ordinal); got != c.want {
			t.Errorf("stableID(%x, %d, %d) = %s, want %s", c.identity, c.seq, c.ordinal, got, c.want)
		}
	}
}

func TestAShortFormEmitsOnTrueAndSkipsOnFalse(t *testing.T) {
	relay := ShortRelay("echo", func(in string, _ *Ctx[NoState, string]) (string, bool) {
		return "hi", in == "go"
	}, ShortOptions{Codec: Text, Output: "out"})
	run, err := StartLocal(relay, WithLogger(quietLog()))
	if err != nil {
		t.Fatal(err)
	}
	var released []string
	if err := run.OnOutput("out", func(out LocalOutput) { released = append(released, string(out.Payload)) }); err != nil {
		t.Fatal(err)
	}
	for _, in := range []string{"skip", "go"} {
		if err := run.Feed("input", []byte(in)); err != nil {
			t.Fatal(err)
		}
	}
	if err := run.RunUntilIdle(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := run.Close(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(released, []string{"hi"}) {
		t.Fatalf("released %v, want nothing for the first input and hi for the second", released)
	}
}
