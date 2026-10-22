package flezha

import (
	"errors"
	"fmt"

	"github.com/flezha/flezha_go_sdk/internal/check"
	"github.com/flezha/flezha_go_sdk/internal/codec"
	"github.com/flezha/flezha_go_sdk/internal/harness"
	"github.com/flezha/flezha_go_sdk/internal/native"
)

func init() {
	harness.New = func(relay any, cellKey *string, relayID string) (harness.Engine, error) {
		r, ok := relay.(Relay)
		if !ok {
			return nil, errors.New("flezha: the harness was handed no relay")
		}
		spec, err := specOf(r)
		if err != nil {
			return nil, err
		}
		class := newRegisteredClass(spec)
		node := spec.tasklets[0]
		key := ""
		if cellKey != nil {
			key = *cellKey
		}
		class.keepIdentity(node, relayID, cellKey, []byte(relayID+"/"+node.name+"/"+key))
		return &engine{class: class, node: node, relay: relayID, cellKey: cellKey, core: &fakeCore{tasklet: node.name}}, nil
	}
}

// engine drives one tasklet through the dispatcher's own invocation path.
type engine struct {
	class     *registeredClass
	node      *taskNode
	relay     string
	cellKey   *string
	core      *fakeCore
	log       [][]byte
	changeSeq uint64
}

func (e *engine) Cell() string {
	if e.cellKey == nil {
		return e.node.name
	}
	return e.node.name + "/" + *e.cellKey
}

func (e *engine) State() any { return e.class.stateOf(e.node, e.relay, e.cellKey) }

func (e *engine) Feed(input any, now uint64, replay bool) (harness.Outcome, error) {
	e.core.beginInput(replay)
	outcome, err := e.drive(now, replay, false, func(inv *invocation) error { return e.node.def.run(inv, input) })
	if err != nil {
		return outcome, err
	}
	return outcome, e.core.endInput()
}

func (e *engine) FireTimer(eventTime bool, scheduledAt uint64) (harness.Outcome, error) {
	kind := ProcessingTime
	if eventTime {
		kind = EventTime
	}
	return e.drive(scheduledAt, false, false, func(inv *invocation) error {
		if !e.node.def.hasTimer {
			return refusal("runtime.timer_without_on_timer", map[string]any{"tasklet": e.node.name})
		}
		return e.node.def.onTimer(inv, TimerFiring{Kind: kind, ScheduledAt: scheduledAt})
	})
}

func (e *engine) Compensate(outputs [][]byte) (harness.Outcome, error) {
	return e.drive(0, false, true, func(inv *invocation) error {
		if !e.node.def.hasCompensate {
			return nil
		}
		return e.node.def.compensate(inv, outputs)
	})
}

func (e *engine) Restore(snapshot []byte, changes [][]byte) error {
	state, err := e.fold(snapshot, changes)
	if err == nil {
		e.class.setState(e.node, e.relay, e.cellKey, state)
	}
	return err
}

func (e *engine) Replayed() (any, error) { return e.fold(nil, e.log) }

func (e *engine) DecodeChange(body []byte) (any, error) {
	return e.node.def.decodeChange(body)
}

// fold is the state the dispatcher's own restore fold rebuilds.
func (e *engine) fold(snapshot []byte, changes [][]byte) (any, error) {
	fold := codec.RestoreFold{}
	if snapshot != nil {
		fold.Snapshot = &codec.RestoreBody{Bytes: snapshot}
	}
	for _, c := range changes {
		fold.Changes = append(fold.Changes, codec.RestoreBody{Bytes: c})
	}
	return restoreFold(e.node.def, fold, e.cellKey)
}

func (e *engine) drive(now uint64, replay, compensating bool, body func(*invocation) error) (harness.Outcome, error) {
	a := codec.Activation{Token: 0, Relay: e.relay, CellKey: e.cellKey, ChangeSeq: e.changeSeq}
	if compensating {
		a.Kind = codec.KindCompensateRun
	}
	inv := newInvocation(a, e.class, e.node, e.core, now, replay, nil, nil)
	err := invokeSafely(inv, func() error { return body(inv) })
	e.core.spawns = map[uint64][]byte{}
	if err != nil {
		return harness.Outcome{}, err
	}
	e.log = append(e.log, inv.changes...)
	e.changeSeq += uint64(len(inv.changes))
	outcome := harness.Outcome{Changes: inv.changes}
	for _, out := range inv.outputs {
		outcome.Outputs = append(outcome.Outputs, out.Payload)
	}
	for _, t := range inv.timers {
		outcome.Timers = append(outcome.Timers, harness.Timer{EventTime: t.Kind == codec.EventTime, ScheduledAt: t.ScheduledAt, Register: t.Register})
	}
	return outcome, nil
}

// fakeCore answers a Ctx inside the harness as the core would for one tasklet
// with no controller behind it.
type fakeCore struct {
	tasklet    string
	spawns     map[uint64][]byte
	spawnCount uint64
	// Each live input's effect results, by name in call order.
	kept      []map[string][][]byte
	replays   int
	effects   map[string][][]byte
	replaying bool
	ordinals  map[string]int
	running   *struct {
		name    string
		ordinal int
	}
}

// beginInput starts an input: a live one keeps its effect results, a replay
// is served the next live input's.
func (f *fakeCore) beginInput(replay bool) {
	if replay {
		f.effects = map[string][][]byte{}
		if f.replays < len(f.kept) {
			f.effects = f.kept[f.replays]
		}
		f.replays++
	} else {
		f.effects = map[string][][]byte{}
		f.kept = append(f.kept, f.effects)
	}
	f.replaying, f.ordinals, f.running = replay, map[string]int{}, nil
}

// endInput requires a replayed input to have asked for every result its live
// run kept.
func (f *fakeCore) endInput() error {
	if !f.replaying {
		return nil
	}
	for name, results := range f.effects {
		if len(results) > f.ordinals[name] {
			return effectReplayMismatch(f.tasklet, name, "was never asked for")
		}
	}
	return nil
}

func effectReplayMismatch(tasklet, name, what string) error {
	return fmt.Errorf("%s: on the replay, effect '%s' %s. A re-run reads an outside call's result only "+
		"through Effect, with the same name in the same order", tasklet, name, what)
}

// SpawnRequest answers at once what a controller would: spawned under the
// given id, or under one shaped like a generated id, and skipped_on_replay on a replay.
func (f *fakeCore) SpawnRequest(_ uint64, factoryID, id string, _ []byte, _ string, replay bool, _ int64) (uint64, error) {
	handle := f.spawnCount
	f.spawnCount++
	if id == "" && !replay {
		const base32 = "abcdefghijklmnopqrstuvwxyz234567"
		var suffix [16]byte
		for i, n := 15, handle; i >= 0; i, n = i-1, n>>5 {
			suffix[i] = base32[n&31]
		}
		id = factoryID + "-" + string(suffix[:])
	}
	outcome := Spawned
	if replay {
		outcome = SkippedOnReplay
	}
	if f.spawns == nil {
		f.spawns = map[uint64][]byte{}
	}
	f.spawns[handle], _ = encodeJSON(SpawnResult{ID: id, Outcome: outcome})
	return handle, nil
}

func (f *fakeCore) SpawnResult(handle, _ uint64) ([]byte, error) {
	if answer, ok := f.spawns[handle]; ok {
		return answer, nil
	}
	return nil, refusal("runtime.spawn_handle_released", map[string]any{"handle": handle})
}

// Part goes nowhere: there is no controller and no listener.
func (f *fakeCore) Part(uint64, uint32, []byte) error { return nil }

func (f *fakeCore) EffectBegin(_ uint64, _ uint32, name string) (int, []byte, error) {
	if name == "" || len(name) > 255 {
		quoted, _ := encodeJSON(name)
		return 0, nil, refusal("effect.bad_name", map[string]any{"name": string(quoted)})
	}
	if f.running != nil {
		return 0, nil, refusal("effect.nested", map[string]any{"name": name})
	}
	ordinal := f.ordinals[name]
	f.ordinals[name] = ordinal + 1
	if results := f.effects[name]; ordinal < len(results) {
		return native.EffectKept, results[ordinal], nil
	}
	if f.replaying {
		return 0, nil, effectReplayMismatch(f.tasklet, name, "ran its function")
	}
	f.running = &struct {
		name    string
		ordinal int
	}{name, ordinal}
	return native.EffectRun, nil, nil
}

func (f *fakeCore) EffectEnd(_ uint64, _ uint32, payload []byte) ([]byte, error) {
	if f.running == nil {
		return nil, errors.New("flezha: an effect end follows no begin that runs")
	}
	f.effects[f.running.name] = append(f.effects[f.running.name], payload)
	f.running = nil
	return nil, nil
}

func (f *fakeCore) EffectRelease(uint64, uint32) error {
	f.running = nil
	return nil
}

func init() {
	check.Derived = func(relay any) ([]byte, error) {
		r, ok := relay.(Relay)
		if !ok {
			return nil, errors.New("flezha: the check was handed no relay")
		}
		spec, err := specOf(r)
		if err != nil {
			return nil, err
		}
		core, err := native.NewContext()
		if err != nil {
			return nil, err
		}
		defer core.Free()
		return core.CheckClass(spec.declaration())
	}
}
