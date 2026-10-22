package flezha

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// definition is a tasklet as the dispatcher sees it: its declaration, and its
// behaviour as functions over untyped values. It is built once per wiring by
// define, the one place that knows the tasklet's I, S and O, and every hook is
// found there by type assertion, so a declaration and its implementation
// cannot disagree.
type definition struct {
	name      string
	spec      Spec
	typ       reflect.Type
	inType    reflect.Type
	outType   reflect.Type
	stateType reflect.Type
	// The first refusal met reading the tasklet; the wiring reports it.
	err error

	hasInit, hasTimer, hasEventTime, hasCellsFor, hasCompensate, migrates bool
	// The tasklet lists change types, so it journals.
	journals bool

	decodeIn   func([]byte) (any, error)
	decodeOut  func([]byte) (any, error)
	initial    func(cellKey *string) any
	run        func(inv *invocation, in any) error
	onTimer    func(inv *invocation, timer TimerFiring) error
	compensate func(inv *invocation, outputs [][]byte) error
	eventTime  func(in any) uint64
	cellsFor   func(in any) []string
	// The state as JSON, and back from a body written at fromVersion (nil: now).
	encodeState func(state any) ([]byte, error)
	decodeState func(body []byte, fromVersion *uint32) (any, error)
	// One change record folded into state, and as the change it holds.
	foldChange   func(state any, body []byte, fromVersion *uint32) (any, error)
	decodeChange func(body []byte) (any, error)
}

// clock is the clock as the core's declaration spells it.
func (d *definition) clock() string {
	switch {
	case d.hasEventTime:
		return "event_time"
	case d.spec.LogicalClock:
		return "logical"
	default:
		return "wall"
	}
}

// define reads a tasklet into a definition named by its Spec.
func define[I, S, O any](t Tasklet[I, S, O]) *definition {
	spec := t.Spec()
	d := defineAs(t, spec, spec.Name)
	if spec.Name == "" && d.err == nil {
		d.err = refusal("topology.tasklet_name_missing", map[string]any{"tasklet": typeName(d.typ)})
	}
	return d
}

// defineAs reads a tasklet into a definition named name.
func defineAs[I, S, O any](t Tasklet[I, S, O], spec Spec, name string) *definition {
	d := &definition{
		name: name, spec: spec, typ: reflect.TypeOf(t),
		inType: typeOf[I](), outType: typeOf[O](), stateType: typeOf[S](),
		decodeIn: decodeJSONInto[I], decodeOut: decodeJSONInto[O],
	}

	d.initial = func(*string) any { var s S; return s }
	if h, ok := any(t).(initStateHook[S]); ok {
		d.hasInit = true
		d.initial = func(cellKey *string) any { return h.InitState(cellKey) }
	}

	changes := Changes[S]{}
	if h, ok := any(t).(changesHook[S]); ok {
		changes = h.Changes()
	}
	d.journals = len(changes) > 0
	names := map[reflect.Type]string{}
	types := map[string]reflect.Type{}
	for changeName, proto := range changes {
		names[reflect.TypeOf(proto)] = changeName
		types[changeName] = reflect.TypeOf(proto)
	}

	if h, ok := any(t).(eventTimeHook[I]); ok {
		d.hasEventTime = true
		d.eventTime = func(in any) uint64 { return h.EventTime(as[I](in)) }
	}
	if h, ok := any(t).(cellsForHook[I]); ok {
		d.hasCellsFor = true
		d.cellsFor = func(in any) []string { return h.CellsFor(as[I](in)) }
	}
	defineCalls(d, t, names)
	defineRecords(d, t, types)
	checkDefinition[S](d)
	return d
}

// defineCalls sets the hooks the dispatcher calls with a context: Run, and
// OnTimer and Compensate when the tasklet implements them.
func defineCalls[I, S, O any](d *definition, t Tasklet[I, S, O], names map[reflect.Type]string) {
	// The context a hook receives, and the state it leaves behind.
	invoke := func(inv *invocation, body func(*Ctx[S, O]) error) error {
		ctx := &Ctx[S, O]{inv: inv, state: as[S](inv.state), names: names, tasklet: d.name}
		defer func() { inv.state = ctx.state }()
		if err := body(ctx); err != nil {
			return err
		}
		return inv.err
	}
	d.run = func(inv *invocation, in any) error {
		return invoke(inv, func(ctx *Ctx[S, O]) error { return t.Run(as[I](in), ctx) })
	}
	if h, ok := any(t).(onTimerHook[S, O]); ok {
		d.hasTimer = true
		d.onTimer = func(inv *invocation, timer TimerFiring) error {
			return invoke(inv, func(ctx *Ctx[S, O]) error { return h.OnTimer(timer, ctx) })
		}
	}
	if h, ok := any(t).(compensateHook[S, O]); ok {
		d.hasCompensate = true
		d.compensate = func(inv *invocation, encoded [][]byte) error {
			outputs := make([]O, 0, len(encoded))
			for _, b := range encoded {
				v, err := decodeJSONInto[O](b)
				if err != nil {
					return err
				}
				outputs = append(outputs, as[O](v))
			}
			return invoke(inv, func(ctx *Ctx[S, O]) error { return h.Compensate(outputs, ctx) })
		}
	}
}

// defineRecords sets how the state and its change records are written and
// read back, through the migration hooks for a record of an older version.
func defineRecords[I, S, O any](d *definition, t Tasklet[I, S, O], types map[string]reflect.Type) {
	name := d.name
	migrateState, hasMigrateState := any(t).(migrateStateHook[S])
	migrateChange, hasMigrateChange := any(t).(migrateChangeHook[S])
	d.migrates = hasMigrateState || hasMigrateChange

	d.encodeState = func(state any) ([]byte, error) {
		out, err := encodeJSON(state)
		if err != nil {
			return nil, unrepresentable(name, err)
		}
		return out, nil
	}
	d.decodeState = func(body []byte, fromVersion *uint32) (any, error) {
		if fromVersion != nil {
			if !hasMigrateState {
				return nil, hookMissing(name, "state", *fromVersion, "MigrateState")
			}
			return migrateState.MigrateState(*fromVersion, body)
		}
		return decodeJSONInto[S](body)
	}
	d.foldChange = func(state any, body []byte, fromVersion *uint32) (any, error) {
		if !d.journals {
			return nil, refusal("runtime.change_without_apply_change", map[string]any{"tasklet": name})
		}
		var change Change[S]
		var err error
		if fromVersion != nil {
			if !hasMigrateChange {
				return nil, hookMissing(name, "change", *fromVersion, "MigrateChange")
			}
			var record changeRecord
			if record, err = readChangeRecord(name, body); err != nil {
				return nil, err
			}
			change, err = migrateChange.MigrateChange(*fromVersion, record.C, record.V)
		} else {
			change, err = decodeChange[S](name, types, body)
		}
		if err != nil {
			return nil, err
		}
		s := as[S](state)
		change.ApplyTo(&s)
		return s, nil
	}
	d.decodeChange = func(body []byte) (any, error) { return decodeChange[S](name, types, body) }
}

// checkDefinition records the first refusal the declaration earns.
func checkDefinition[S any](d *definition) {
	fail := func(err error) {
		if d.err == nil {
			d.err = err
		}
	}
	tasklet := map[string]any{"tasklet": d.name}
	if d.journals && d.stateType == typeOf[NoState]() {
		fail(refusal("topology.change_without_state", tasklet))
	}
	// A state that cannot be encoded cannot be restored, and the first anyone
	// would learn of it is a restart.
	if _, err := encodeJSON(*new(S)); err != nil {
		fail(unrepresentable(d.name, err))
	}
	if d.spec.LogicalClock && d.hasEventTime {
		fail(refusal("topology.two_clocks", tasklet))
	}
	if d.spec.CellKeys != nil && len(d.spec.CellKeys) == 0 {
		fail(refusal("topology.static_tasklet_without_cell_keys", tasklet))
	}
}

// changeRecord is one change as its record holds it: the listed name, never
// the Go type's, and the value.
type changeRecord struct {
	C string          `json:"c"`
	V json.RawMessage `json:"v"`
}

// encodeChange is a change's record, refused when its type is not listed.
func encodeChange[S any](tasklet string, names map[reflect.Type]string, change Change[S]) ([]byte, error) {
	t := reflect.TypeOf(change)
	name, ok := names[t]
	if !ok && t != nil && t.Kind() == reflect.Pointer {
		name, ok = names[t.Elem()]
	}
	if !ok && t != nil {
		name, ok = names[reflect.PointerTo(t)]
	}
	if !ok {
		return nil, refusal("runtime.change_not_registered", map[string]any{"tasklet": tasklet, "type": typeName(t)})
	}
	value, err := encodeJSON(change)
	if err != nil {
		return nil, unrepresentable(tasklet, err)
	}
	return encodeJSON(changeRecord{C: name, V: value})
}

// decodeChange is the change a record holds, as the type listed under its name.
func decodeChange[S any](tasklet string, types map[string]reflect.Type, body []byte) (Change[S], error) {
	record, err := readChangeRecord(tasklet, body)
	if err != nil {
		return nil, err
	}
	t, ok := types[record.C]
	if !ok {
		return nil, refusal("runtime.change_not_registered", map[string]any{"tasklet": tasklet, "type": record.C})
	}
	ptr := reflect.New(t)
	if err := json.Unmarshal(record.V, ptr.Interface()); err != nil {
		return nil, fmt.Errorf("change '%s' of tasklet '%s' does not decode: %w", record.C, tasklet, err)
	}
	return ptr.Elem().Interface().(Change[S]), nil
}

// readChangeRecord is a change record's listed name and value.
func readChangeRecord(tasklet string, body []byte) (changeRecord, error) {
	var record changeRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return record, fmt.Errorf("a change record of tasklet '%s' is not a named change: %w", tasklet, err)
	}
	return record, nil
}

func unrepresentable(tasklet string, err error) error {
	return refusal("topology.state_unrepresentable", map[string]any{"path": tasklet + ".<root>", "reason": err.Error()})
}

func hookMissing(tasklet, which string, fromVersion uint32, hook string) error {
	return refusal("runtime.hook_missing", map[string]any{
		"tasklet": tasklet,
		"action":  fmt.Sprintf("was handed a %s record written at schema version %d", which, fromVersion),
		"hook":    hook,
	})
}
