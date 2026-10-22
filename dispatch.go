package flezha

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/flezha/flezha_go_sdk/internal/codec"
)

// registry is the relay classes a runner serves, by factory id.
type registry struct {
	mu      sync.RWMutex
	classes map[string]*registeredClass
}

func newRegistry() *registry { return &registry{classes: map[string]*registeredClass{}} }

func (r *registry) add(spec *classSpec) (*registeredClass, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, taken := r.classes[spec.relay.FactoryID]; taken {
		return nil, refusal("topology.duplicate_factory_id", map[string]any{"factory_id": spec.relay.FactoryID})
	}
	class := newRegisteredClass(spec)
	r.classes[spec.relay.FactoryID] = class
	return class, nil
}

func (r *registry) remove(factoryID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.classes, factoryID)
}

func (r *registry) get(factoryID string) *registeredClass {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.classes[factoryID]
}

// registeredClass is a declared class, with the lookups dispatch needs and
// the live state of every cell it owns. Per-cell exclusivity is the core's:
// no two activations of one cell are ever in flight, so the lock guards the
// maps and never a cell.
type registeredClass struct {
	spec       *classSpec
	outgoing   [][]*channelSpec
	mu         sync.Mutex
	cells      map[cellAddress]any
	identities map[cellAddress][]byte
}

// cellAddress is one cell: a class serves every relay of its factory on this
// runner, and each has cells of its own under the same keys.
type cellAddress struct {
	relay string
	node  int
	keyed bool
	key   string
}

func address(node *taskNode, relay string, cellKey *string) cellAddress {
	a := cellAddress{relay: relay, node: node.index}
	if cellKey != nil {
		a.keyed, a.key = true, *cellKey
	}
	return a
}

func newRegisteredClass(spec *classSpec) *registeredClass {
	outgoing := make([][]*channelSpec, len(spec.tasklets))
	for _, ch := range spec.channels {
		if ch.producer != nil {
			outgoing[*ch.producer] = append(outgoing[*ch.producer], ch)
		}
	}
	return &registeredClass{
		spec: spec, outgoing: outgoing,
		cells: map[cellAddress]any{}, identities: map[cellAddress][]byte{},
	}
}

// stateOf is the cell's live state, created from the declaration on first touch.
func (c *registeredClass) stateOf(node *taskNode, relay string, cellKey *string) any {
	a := address(node, relay, cellKey)
	c.mu.Lock()
	state, ok := c.cells[a]
	c.mu.Unlock()
	if ok {
		return state
	}
	state = node.def.initial(cellKey)
	c.setState(node, relay, cellKey, state)
	return state
}

func (c *registeredClass) setState(node *taskNode, relay string, cellKey *string, state any) {
	c.mu.Lock()
	c.cells[address(node, relay, cellKey)] = state
	c.mu.Unlock()
}

func (c *registeredClass) keepIdentity(node *taskNode, relay string, cellKey *string, identity []byte) {
	c.mu.Lock()
	c.identities[address(node, relay, cellKey)] = identity
	c.mu.Unlock()
}

func (c *registeredClass) identityOf(node *taskNode, relay string, cellKey *string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	identity, ok := c.identities[address(node, relay, cellKey)]
	return identity, ok
}

// forget drops a cell that left this runner, or every cell of its relay.
func (c *registeredClass) forget(node *taskNode, relay string, cellKey *string, wholeRelay bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !wholeRelay {
		a := address(node, relay, cellKey)
		delete(c.cells, a)
		delete(c.identities, a)
		return
	}
	for a := range c.cells {
		if a.relay == relay {
			delete(c.cells, a)
		}
	}
	for a := range c.identities {
		if a.relay == relay {
			delete(c.identities, a)
		}
	}
}

// completer is what a dispatcher answers activations through.
type completer interface {
	coreBridge
	Complete(result []byte) error
	Abandon(token uint64) error
}

// dispatcher turns one activation into one answer, exactly once.
type dispatcher struct {
	registry *registry
	core     completer
	log      *slog.Logger
}

// dispatch answers one activation: a completion, or an abandon when no answer
// could be completed. An activation left unanswered would retire its cell for
// the life of the process.
func (d *dispatcher) dispatch(encoded []byte) {
	a, err := codec.DecodeActivation(encoded)
	if err != nil {
		d.log.Error("an activation did not decode", "error", err)
		if token, terr := codec.TokenOf(encoded); terr == nil {
			d.abandon(token)
		}
		return
	}
	if err := d.core.Complete(codec.EncodeResult(d.answer(a))); err != nil {
		d.log.Error("completing an activation failed", "token", a.Token, "error", err)
		d.abandon(a.Token)
	}
}

func (d *dispatcher) abandon(token uint64) {
	// The core refuses an unknown token: a completion already landed.
	if err := d.core.Abandon(token); err != nil {
		d.log.Warn("abandoning an activation failed", "token", token, "error", err)
	}
}

// answer produces the result for one activation; every failure becomes a
// result the core can act on.
func (d *dispatcher) answer(a codec.Activation) (result codec.Result) {
	var err error
	defer func() {
		if err != nil {
			result = failed(a.Token, err.Error(), isNonRetryable(err))
		}
	}()
	defer recovered(&err)
	class := d.registry.get(a.Factory)
	if class == nil {
		return failed(a.Token, fmt.Sprintf("no class registered for factory `%s`", a.Factory), true)
	}
	if int(a.Tasklet) >= len(class.spec.tasklets) {
		return failed(a.Token, fmt.Sprintf("class `%s` has no tasklet at index %d", a.Factory, a.Tasklet), true)
	}
	node := class.spec.tasklets[a.Tasklet]
	if a.CellIdentity != nil {
		class.keepIdentity(node, a.Relay, a.CellKey, a.CellIdentity)
	}
	switch a.Kind {
	case codec.KindWork:
		return d.runWork(a, class, node)
	case codec.KindRestoreFold:
		var state any
		if state, err = restoreFold(node.def, a.Restore, a.CellKey); err == nil {
			class.setState(node, a.Relay, a.CellKey, state)
			return codec.Result{Token: a.Token, Outcome: codec.OutcomeRestored}
		}
	case codec.KindSnapshotRequest, codec.KindLiveStateRead:
		var body []byte
		if body, err = node.def.encodeState(class.stateOf(node, a.Relay, a.CellKey)); err == nil {
			outcome := codec.OutcomeStateJSON
			if a.Kind == codec.KindSnapshotRequest {
				outcome = codec.OutcomeSnapshot
			}
			return codec.Result{Token: a.Token, Outcome: outcome, Bytes: body}
		}
	case codec.KindApplyState:
		var state any
		if state, err = node.def.decodeState(a.StateJSON, nil); err == nil {
			class.setState(node, a.Relay, a.CellKey, state)
			return codec.Result{Token: a.Token, Outcome: codec.OutcomeStateApplied}
		}
	case codec.KindKeyRequest:
		return codec.Result{Token: a.Token, Outcome: codec.OutcomePrepared, Prepared: prepare(class, a.KeyRequest)}
	case codec.KindCompensateRun:
		return d.compensate(a, class, node)
	case codec.KindLeave:
		class.forget(node, a.Relay, a.CellKey, a.WholeRelay)
		return codec.Result{Token: a.Token, Outcome: codec.OutcomeStateApplied}
	default:
		return failed(a.Token, fmt.Sprintf("unknown activation kind %d", a.Kind), true)
	}
	return result
}

// restoreFold is the declared starting state, the snapshot when there is one,
// then every change folded on top in order: the one fold a restore and the
// test harness both run.
func restoreFold(def *definition, fold codec.RestoreFold, cellKey *string) (any, error) {
	state := def.initial(cellKey)
	var err error
	if fold.Snapshot != nil {
		if state, err = def.decodeState(fold.Snapshot.Bytes, fold.Snapshot.FromVersion); err != nil {
			return nil, err
		}
	}
	for _, body := range fold.Changes {
		if state, err = def.foldChange(state, body.Bytes, body.FromVersion); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func (d *dispatcher) runWork(a codec.Activation, class *registeredClass, node *taskNode) codec.Result {
	effects := make([]codec.ItemEffect, 0, len(a.Items))
	for index, item := range a.Items {
		effect := d.runItem(a, class, node, item, uint32(index))
		effects = append(effects, effect)
		// Items after a failure are re-handed by the core.
		if effect.Verdict != codec.Ok {
			break
		}
	}
	return codec.Result{Token: a.Token, Outcome: codec.OutcomeWork, Effects: effects}
}

// unreadable is an entry payload the channel's decoder or its edge rejected:
// the core parks it rather than counting it a tasklet failure.
type unreadable struct{ error }

func (d *dispatcher) runItem(a codec.Activation, class *registeredClass, node *taskNode, item codec.WorkItem, index uint32) codec.ItemEffect {
	now := item.Now
	if item.Timer {
		now = item.ScheduledAt
	}
	inv := newInvocation(a, class, node, d.core, now, item.Replay, item.ConsumedOffset, &index)
	if err := invokeSafely(inv, func() error { return serve(inv, class, node, item) }); err != nil {
		d.log.Debug("a tasklet failed an item", "tasklet", node.name, "error", err)
		verdict := codec.Retryable
		var bad unreadable
		switch {
		case errors.As(err, &bad):
			verdict = codec.Unreadable
		case isNonRetryable(err):
			verdict = codec.NonRetryable
		}
		return codec.ItemEffect{Verdict: verdict, Message: err.Error(), EventTime: now, ConsumedOffset: item.ConsumedOffset}
	}
	return inv.effect(item.ConsumedOffset)
}

// newInvocation is an item's context over the cell's live state.
func newInvocation(a codec.Activation, class *registeredClass, node *taskNode, bridge coreBridge, now uint64, replay bool, offset *uint64, item *uint32) *invocation {
	return &invocation{
		class: class, node: node, relay: a.Relay, cellKey: a.CellKey, changeSeq: a.ChangeSeq,
		now: now, replay: replay, state: class.stateOf(node, a.Relay, a.CellKey), bridge: bridge,
		token: a.Token, offset: offset, item: item, compensating: a.Kind == codec.KindCompensateRun,
	}
}

// invokeSafely runs body, recovering a panic as a retryable error carrying
// its text, and keeps the state the body left whether or not it failed: a
// change recorded before the failure is already journaled.
func invokeSafely(inv *invocation, body func() error) (err error) {
	defer func() { inv.class.setState(inv.node, inv.relay, inv.cellKey, inv.state) }()
	defer recovered(&err)
	return body()
}

// recovered turns a panic in the deferring function into its error, retryable
// and carrying the panic's text, as an exception is in the other SDKs.
func recovered(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("panic: %v", r)
	}
}

// serve hands one item to user code: a timer firing, or each decoded input.
func serve(inv *invocation, class *registeredClass, node *taskNode, item codec.WorkItem) error {
	def := node.def
	if item.Timer {
		if !def.hasTimer {
			return refusal("runtime.timer_without_on_timer", map[string]any{"tasklet": node.name})
		}
		kind := ProcessingTime
		if item.TimerKind == codec.EventTime {
			kind = EventTime
		}
		return def.onTimer(inv, TimerFiring{Kind: kind, ScheduledAt: item.ScheduledAt})
	}
	values, err := inputValues(class, node, item)
	if err != nil {
		return err
	}
	for _, v := range values {
		if err := def.run(inv, v); err != nil {
			return err
		}
	}
	return nil
}

// inputValues is a delivered payload as the tasklet sees it. A prepared one
// carries its values already through this node's edge, a transformed one is
// the producer's own output for this consumer, and an untransformed one is
// decoded by its channel and run through this node's edge. A failure on an
// entry channel is the entrance's, and is reported as unreadable.
func inputValues(class *registeredClass, node *taskNode, item codec.WorkItem) ([]any, error) {
	def := node.def
	switch item.Form {
	case codec.Prepared:
		values := make([]any, 0, len(item.Values))
		for _, b := range item.Values {
			v, err := def.decodeIn(b)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		return values, nil
	case codec.Transformed:
		v, err := def.decodeIn(item.Payload)
		if err != nil {
			return nil, err
		}
		return []any{v}, nil
	}
	ch := class.channel(item.Channel)
	values, err := untransformed(ch, node, item.Payload)
	if err != nil && (ch == nil || ch.kind == "entry") {
		return nil, unreadable{err}
	}
	return values, err
}

// untransformed decodes a payload as its channel reads it and runs it through
// node's own edge off that channel.
func untransformed(ch *channelSpec, node *taskNode, payload []byte) (values []any, err error) {
	defer recovered(&err)
	if ch == nil {
		v, err := node.def.decodeIn(payload)
		return []any{v}, err
	}
	v, err := ch.decode(payload)
	if err != nil {
		return nil, err
	}
	return applyTransforms(edgeInto(ch, node.index).transforms, v), nil
}

// channel is the class's channel at index, or nil for one it does not declare.
func (c *registeredClass) channel(index uint32) *channelSpec {
	if int(index) < len(c.spec.channels) {
		return c.spec.channels[index]
	}
	return nil
}

// edgeInto is the channel's edge feeding node, or an empty one.
func edgeInto(ch *channelSpec, node int) *consumerEdge {
	for _, edge := range ch.consumers {
		if edge.tasklet == node {
			return edge
		}
	}
	return &consumerEdge{}
}

// prepare answers the crossing: per payload, every value each keyed consumer's
// own edge yields with the cells it names, and the event time the first timed
// consumer reads off its first value. A per-payload failure enters the input
// channel's retry ladder; the other payloads still have answers.
func prepare(class *registeredClass, request codec.KeyRequest) []codec.PrepareOutcome {
	ch := class.channel(request.Channel)
	answers := make([]codec.PrepareOutcome, 0, len(request.Payloads))
	for _, payload := range request.Payloads {
		answer, err := prepared(class, ch, request, payload)
		if err != nil {
			answer = codec.PrepareOutcome{Failed: true, Error: codec.WorkError{Message: err.Error(), NonRetryable: isNonRetryable(err)}}
		}
		answers = append(answers, answer)
	}
	return answers
}

func prepared(class *registeredClass, ch *channelSpec, request codec.KeyRequest, payload []byte) (answer codec.PrepareOutcome, err error) {
	defer recovered(&err)
	if ch == nil {
		return answer, nil
	}
	value, err := ch.decode(payload)
	if err != nil {
		return answer, err
	}
	for _, edge := range ch.consumers {
		def := class.spec.tasklets[edge.tasklet].def
		keyed := request.WantKeys && (edge.keyBy != nil || def.hasCellsFor)
		timed := request.WantEventTime && answer.EventTime == nil && def.hasEventTime
		if !keyed && !timed {
			continue
		}
		yielded := applyTransforms(edge.transforms, value)
		if timed && len(yielded) > 0 {
			t := def.eventTime(yielded[0])
			answer.EventTime = &t
		}
		if !keyed {
			continue
		}
		for _, v := range yielded {
			var keys []string
			if edge.keyBy != nil {
				keys = []string{edge.keyBy(v)}
			} else {
				keys = def.cellsFor(v)
			}
			bytes, err := encodeJSON(v)
			if err != nil {
				return answer, err
			}
			answer.Values = append(answer.Values, codec.PreparedValue{Consumer: uint32(edge.tasklet), Keys: keys, Bytes: bytes})
		}
	}
	return answer, nil
}

func (d *dispatcher) compensate(a codec.Activation, class *registeredClass, node *taskNode) codec.Result {
	if !node.def.hasCompensate {
		return failed(a.Token, node.name+" declares no compensate hook", true)
	}
	inv := newInvocation(a, class, node, d.core, 0, false, nil, nil)
	if err := invokeSafely(inv, func() error { return node.def.compensate(inv, a.Outputs) }); err != nil {
		return failed(a.Token, err.Error(), isNonRetryable(err))
	}
	return codec.Result{Token: a.Token, Outcome: codec.OutcomeWork, Effects: []codec.ItemEffect{inv.effect(nil)}}
}

func failed(token uint64, message string, nonRetryable bool) codec.Result {
	return codec.Result{Token: token, Outcome: codec.OutcomeFailed, Error: codec.WorkError{Message: message, NonRetryable: nonRetryable}}
}
