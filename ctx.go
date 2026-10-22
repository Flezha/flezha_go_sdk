package flezha

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/flezha/flezha_go_sdk/internal/codec"
	"github.com/flezha/flezha_go_sdk/internal/native"
)

// Ctx is the invocation context: every deterministic effect a tasklet is
// allowed. It accumulates one item's effects (recorded changes, emitted
// values, timer operations, the checkpoint) and the runner turns them into the
// record the core folds; nothing reaches the wire on its own.
//
// A failing call returns its error; Run returning it fails the item, as an
// uncaught exception does in the other SDKs.
type Ctx[S, O any] struct {
	inv     *invocation
	state   S
	names   map[reflect.Type]string
	tasklet string
}

// invocation is one item's effects, untyped, as the dispatcher reads them.
type invocation struct {
	class        *registeredClass
	node         *taskNode
	relay        string
	cellKey      *string
	changeSeq    uint64
	now          uint64
	replay       bool
	state        any
	bridge       coreBridge
	token        uint64
	offset       *uint64
	compensating bool
	item         *uint32
	ordinal      uint32

	changes      [][]byte
	outputs      []codec.Output
	timers       []codec.TimerOp
	checkpointed bool
	completion   []byte
	// A failure of a call that has no error to return it through (StableID).
	err error
}

// coreBridge is what a Ctx asks the core for: a spawn, a part, an effect. The
// runner's is the core itself, the test harness's a stand-in.
type coreBridge interface {
	SpawnRequest(token uint64, factoryID, id string, payload []byte, ifRunning string, replay bool, offset int64) (uint64, error)
	SpawnResult(handle, timeoutMs uint64) ([]byte, error)
	Part(token uint64, item uint32, payload []byte) error
	EffectBegin(token uint64, item uint32, name string) (int, []byte, error)
	EffectEnd(token uint64, item uint32, payload []byte) ([]byte, error)
	EffectRelease(token uint64, item uint32) error
}

// fail keeps the first failure of a call that cannot return one.
func (inv *invocation) fail(err error) {
	if inv.err == nil {
		inv.err = err
	}
}

// effect is this item's effect record, as the core folds it.
func (inv *invocation) effect(consumedOffset *uint64) codec.ItemEffect {
	return codec.ItemEffect{
		Verdict: codec.Ok, Changes: inv.changes, Outputs: inv.outputs, ConsumedOffset: consumedOffset,
		EventTime: inv.now, Timers: inv.timers, Checkpoint: inv.checkpointed, Complete: inv.completion,
	}
}

// State is a copy of the cell's state: assigning a field of it changes
// nothing durable. Maps, slices and pointers inside it are shared and not
// guarded; change state only through Change.
func (c *Ctx[S, O]) State() S { return c.state }

// Change records one change: journaled, then folded into the state through
// the same ApplyTo a restore uses. A change type the tasklet's Changes does not
// list is refused with runtime.change_not_registered.
func (c *Ctx[S, O]) Change(change Change[S]) error {
	body, err := encodeChange(c.tasklet, c.names, change)
	if err != nil {
		return err
	}
	c.inv.changes = append(c.inv.changes, body)
	change.ApplyTo(&c.state)
	return nil
}

// EmitOption qualifies one Emit.
type EmitOption func(*emitOptions)

type emitOptions struct {
	key      *string
	channel  *string
	metadata map[string]string
}

// WithKey emits under key instead of the edge's own.
func WithKey(key string) EmitOption { return func(o *emitOptions) { o.key = &key } }

// WithChannel emits on the named channel this tasklet produces on.
func WithChannel(name string) EmitOption { return func(o *emitOptions) { o.channel = &name } }

// WithMetadata attaches metadata; a key in the reserved `flz.` namespace is refused.
func WithMetadata(metadata map[string]string) EmitOption {
	return func(o *emitOptions) { o.metadata = metadata }
}

// Emit sends one value on the tasklet's output: to each consumer through its
// own edge, and to the channel's output or call when it ends in one.
func (c *Ctx[S, O]) Emit(value O, opts ...EmitOption) error {
	var o emitOptions
	for _, opt := range opts {
		opt(&o)
	}
	return c.inv.emit(value, o)
}

// Part sends value as a part: a preview of the output this invocation is
// producing, which leaves now rather than when Run returns. It never waits; a
// part that cannot leave at once is dropped, and a replay sends none.
func (c *Ctx[S, O]) Part(value O) error {
	inv := c.inv
	var payload []byte
	if out := inv.class.outgoing[inv.node.index]; len(out) > 0 && out[0].sink != nil {
		encoded, err := out[0].sinkEncode(value)
		if err != nil {
			return err
		}
		payload = encoded
	}
	var item uint32
	if inv.item != nil {
		item = *inv.item
	}
	return inv.bridge.Part(inv.token, item, payload)
}

// Now is the core-evaluated clock for this item, never ambient time.
func (c *Ctx[S, O]) Now() uint64 { return c.inv.now }

// StableID is a replay-stable identifier, distinct per call within the
// invocation: the n-th call of a replayed invocation equals the n-th of the
// original.
func (c *Ctx[S, O]) StableID() string {
	inv := c.inv
	identity, ok := inv.class.identityOf(inv.node, inv.relay, inv.cellKey)
	if !ok {
		inv.fail(errors.New("flezha: the core handed this runner no identity for the cell"))
		return ""
	}
	id := stableID(identity, inv.changeSeq, inv.ordinal)
	inv.ordinal++
	return id
}

// CellKey is the cell's key, and false for a single-cell tasklet.
func (c *Ctx[S, O]) CellKey() (string, bool) {
	if c.inv.cellKey == nil {
		return "", false
	}
	return *c.inv.cellKey, true
}

// Replay answers whether this invocation replays an input already processed.
func (c *Ctx[S, O]) Replay() bool { return c.inv.replay }

// TimerAfter registers a processing-time timer ms after Now, and answers its time.
func (c *Ctx[S, O]) TimerAfter(ms uint64) (uint64, error) {
	at := c.inv.now + ms
	return at, c.inv.timer(codec.ProcessingTime, at, true)
}

// TimerAtEventTime registers an event-time timer at at, and answers it.
func (c *Ctx[S, O]) TimerAtEventTime(at uint64) (uint64, error) {
	return at, c.inv.timer(codec.EventTime, at, true)
}

// CancelTimer cancels a registered timer by the time its registration answered.
func (c *Ctx[S, O]) CancelTimer(kind TimerKind, at uint64) error {
	k := codec.ProcessingTime
	if kind == EventTime {
		k = codec.EventTime
	}
	return c.inv.timer(k, at, false)
}

func (inv *invocation) timer(kind codec.TimerKind, at uint64, register bool) error {
	// A rollback is no place to arm a deadline.
	if inv.compensating {
		return refusal("runtime.timers_unavailable", nil)
	}
	inv.timers = append(inv.timers, codec.TimerOp{Kind: kind, ScheduledAt: at, Register: register})
	return nil
}

// Checkpoint closes the open checkpoint interval (checkpoint ack-granularity classes).
func (c *Ctx[S, O]) Checkpoint() { c.inv.checkpointed = true }

// Complete ends the instance with result once the current iteration commits;
// a later call replaces an earlier one. The result is sent as an output naming
// no codec sends a value, and nil is no result.
func (c *Ctx[S, O]) Complete(result any) error {
	if result == nil {
		c.inv.completion = []byte{}
		return nil
	}
	encoded, err := plain(result, "tasklet '"+c.tasklet+"'")
	if err != nil {
		return err
	}
	c.inv.completion = encoded
	return nil
}

// SpawnOption qualifies one SpawnSteps.
type SpawnOption func(*spawnOptions)

type spawnOptions struct{ id, ifRunning string }

// SpawnID names the instance, so a reissued request reaches the same one.
func SpawnID(id string) SpawnOption { return func(o *spawnOptions) { o.id = id } }

// IfRunning says what a running instance under SpawnID gets: "deliver" (the
// default) feeds it the payload, "error" leaves it alone and the read fails.
func IfRunning(policy string) SpawnOption { return func(o *spawnOptions) { o.ifRunning = policy } }

// SpawnSteps asks the controller for a steps instance of factoryID with
// payload as its first input, and answers the handle at once. A replayed
// invocation sends nothing and its handle answers skipped_on_replay.
func (c *Ctx[S, O]) SpawnSteps(factoryID string, payload any, opts ...SpawnOption) (*SpawnHandle, error) {
	var o spawnOptions
	for _, opt := range opts {
		opt(&o)
	}
	inv := c.inv
	encoded, err := plain(payload, "tasklet '"+c.tasklet+"'")
	if err != nil {
		return nil, err
	}
	offset := int64(-1)
	if inv.offset != nil {
		offset = int64(*inv.offset)
	}
	handle, err := inv.bridge.SpawnRequest(inv.token, factoryID, o.id, encoded, o.ifRunning, inv.replay, offset)
	if err != nil {
		return nil, err
	}
	return &SpawnHandle{bridge: inv.bridge, handle: handle}, nil
}

// Effect runs fn, a call to anything outside the instance, and keeps its
// result: when this invocation runs again (a crash re-run, a replay, a move)
// the call returns the kept result and fn does not run. name names the call,
// counted per invocation. A string or []byte result is kept as it stands and
// anything else as JSON; an invocation with no input-log offset runs fn each
// time and keeps nothing.
func Effect[T, S, O any](ctx *Ctx[S, O], name string, fn func() (T, error)) (T, error) {
	inv := ctx.inv
	var zero T
	var item uint32
	if inv.item != nil {
		item = *inv.item
	}
	status, kept, err := inv.bridge.EffectBegin(inv.token, item, name)
	if err != nil {
		return zero, effectRefusal(err)
	}
	if status == native.EffectKept {
		return decodeEffect[T](kept)
	}
	defer func() { _ = inv.bridge.EffectRelease(inv.token, item) }()
	value, err := fn()
	if err != nil {
		return zero, err
	}
	encoded, err := plain(value, "effect '"+name+"'")
	if err != nil {
		return zero, err
	}
	if status == native.EffectRun {
		first, err := inv.bridge.EffectEnd(inv.token, item, encoded)
		if err != nil {
			return zero, effectRefusal(err)
		}
		if first != nil {
			return decodeEffect[T](first)
		}
	}
	return decodeEffect[T](encoded)
}

// effectRefusal marks an effect.* refusal non-retryable, but for
// effect.not_recorded: a call that could not be kept is the one worth running again.
func effectRefusal(err error) error {
	var refused *Error
	if errors.As(err, &refused) && strings.HasPrefix(refused.Code, "effect.") && refused.Code != "effect.not_recorded" {
		return NonRetryable(err)
	}
	return err
}

// decodeEffect is a kept result as T: a string or []byte as it stands, else
// JSON; an empty interface T reads JSON, else text, else bytes, as it was kept.
func decodeEffect[T any](body []byte) (T, error) {
	var out T
	switch p := any(&out).(type) {
	case *string:
		*p = string(body)
	case *[]byte:
		*p = append([]byte(nil), body...)
	default:
		err := json.Unmarshal(body, &out)
		if t := typeOf[T](); err == nil || t.Kind() != reflect.Interface || t.NumMethod() > 0 {
			return out, err
		}
		if utf8.Valid(body) {
			return any(string(body)).(T), nil
		}
		return any(append([]byte(nil), body...)).(T), nil
	}
	return out, nil
}

// emit releases one value on the node's outgoing channels.
func (inv *invocation) emit(value any, o emitOptions) error {
	var targets []*channelSpec
	for _, ch := range inv.class.outgoing[inv.node.index] {
		if o.channel == nil || ch.name == *o.channel {
			targets = append(targets, ch)
		}
	}
	if len(targets) == 0 {
		if o.channel == nil {
			return refusal("runtime.no_output_channel", map[string]any{"tasklet": inv.node.name})
		}
		return refusal("runtime.channel_not_produced", map[string]any{"tasklet": inv.node.name, "channel": *o.channel})
	}
	metadata, err := userMetadata(o.metadata)
	if err != nil {
		return err
	}
	for _, ch := range targets {
		if err := inv.release(ch, value, o.key, metadata); err != nil {
			return err
		}
	}
	return nil
}

// release is one channel's outputs for value. In steps a value rides the edge
// path to its first consumer untransformed, and a terminal the core derived
// inter-tasklet hands it to the core as the pass's handoff; in a flow each
// consumer's own edge transforms, keys and times it producer-side.
func (inv *invocation) release(ch *channelSpec, value any, key *string, metadata [][2]string) error {
	if inv.class.spec.steps {
		consumer := -1
		if len(ch.consumers) > 0 {
			consumer = ch.consumers[0].tasklet
		} else if ch.derivedKind == "inter_task" && ch.producer != nil {
			consumer = *ch.producer
		}
		if consumer >= 0 {
			payload, err := edgePayload(ch, value)
			if err != nil {
				return err
			}
			inv.outputs = append(inv.outputs, codec.Output{Channel: uint32(ch.index), Consumer: uint32(consumer), Payload: payload})
		}
	} else {
		for _, edge := range ch.consumers {
			consumer := inv.class.spec.tasklets[edge.tasklet].def
			for _, v := range applyTransforms(edge.transforms, value) {
				resolved := key
				if resolved == nil && edge.keyBy != nil {
					k := edge.keyBy(v)
					resolved = &k
				}
				resolved = inv.orCellKey(resolved)
				var eventTime *uint64
				if consumer.hasEventTime {
					t := consumer.eventTime(v)
					eventTime = &t
				}
				payload, err := edgePayload(ch, v)
				if err != nil {
					return err
				}
				inv.outputs = append(inv.outputs, codec.Output{
					Channel: uint32(ch.index), Consumer: uint32(edge.tasklet), Key: resolved,
					EventTime: eventTime, Metadata: metadata, Payload: payload,
				})
			}
		}
	}
	if ch.sink != nil {
		resolved := inv.orCellKey(key)
		if ch.keyed && resolved == nil {
			return refusal("runtime.key_required_on_keyed_channel", map[string]any{"channel": ch.name})
		}
		payload, err := ch.sinkEncode(value)
		if err != nil {
			return err
		}
		inv.outputs = append(inv.outputs, codec.Output{Plugin: true, PluginChannel: ch.name, Key: resolved, Metadata: metadata, Payload: payload})
	}
	if call := ch.call; call != nil {
		for _, request := range applyTransforms(call.transforms, value) {
			var resolved *string
			if call.keyBy != nil {
				k := call.keyBy(request)
				resolved = &k
			}
			payload, err := call.encode(request)
			if err != nil {
				return err
			}
			inv.outputs = append(inv.outputs, codec.Output{Plugin: true, PluginChannel: ch.name, Key: resolved, Metadata: metadata, Payload: payload})
		}
	}
	return nil
}

// orCellKey is key, or the invocation's cell key when there is none.
func (inv *invocation) orCellKey(key *string) *string {
	if key == nil {
		return inv.cellKey
	}
	return key
}

// edgePayload is an inter-tasklet frame's bytes: JSON, always, which the
// consuming tasklet decodes.
func edgePayload(ch *channelSpec, value any) ([]byte, error) {
	out, err := encodeJSON(value)
	if err != nil {
		return nil, unrepresentable(ch.name, err)
	}
	return out, nil
}

// userMetadata is metadata as the wire carries it, in key order, refusing a
// key in the reserved namespace.
func userMetadata(m map[string]string) ([][2]string, error) {
	if len(m) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		if strings.HasPrefix(k, "flz.") {
			return nil, refusal("runtime.reserved_metadata_key", map[string]any{"key": k})
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([][2]string, len(keys))
	for i, k := range keys {
		pairs[i] = [2]string{k, m[k]}
	}
	return pairs, nil
}

// applyTransforms runs an edge's transform chain over one value: a dropped
// value is the empty list, a mapped one a list of one, a fanned one many.
func applyTransforms(transforms []func(any) []any, value any) []any {
	values := []any{value}
	for _, t := range transforms {
		var next []any
		for _, v := range values {
			next = append(next, t(v)...)
		}
		if len(next) == 0 {
			return nil
		}
		values = next
	}
	return values
}
