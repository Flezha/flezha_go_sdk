// Package codec is the `Activation` / `ActivationResult` boundary contract in
// Go: postcard, field for field as `crates/flz_core/src/schema.rs` declares
// it, at native.SchemaVersion.
//
// Field order and variant index are the wire contract. Every integer is a
// LEB128 varint; a `u64` decodes into a uint64.
package codec

import "fmt"

// Kind is an ActivationKind variant, in declaration order.
type Kind uint8

const (
	KindWork Kind = iota
	KindRestoreFold
	KindSnapshotRequest
	KindLiveStateRead
	KindApplyState
	KindKeyRequest
	KindCompensateRun
	KindLeave
)

// TimerKind is a TimerKind variant, in declaration order.
type TimerKind uint8

const (
	EventTime TimerKind = iota
	ProcessingTime
)

// String is the kind as the SDKs spell it.
func (k TimerKind) String() string {
	if k == EventTime {
		return "event_time"
	}
	return "processing_time"
}

// InputForm is an InputForm variant, in declaration order.
type InputForm uint8

const (
	Untransformed InputForm = iota
	Transformed
	Prepared
)

// Activation is one unit of work the core hands out.
type Activation struct {
	Token   uint64
	Relay   string
	Tasklet uint32
	CellKey *string
	Kind    Kind

	Items      []WorkItem  // KindWork
	Restore    RestoreFold // KindRestoreFold
	StateJSON  []byte      // KindApplyState
	KeyRequest KeyRequest  // KindKeyRequest
	Outputs    [][]byte    // KindCompensateRun
	WholeRelay bool        // KindLeave

	ChangeSeq    uint64
	Factory      string
	CellIdentity []byte // nil when the activation carries none
}

// WorkItem is an input or a timer firing.
type WorkItem struct {
	Timer          bool
	Channel        uint32
	Payload        []byte
	Metadata       [][2]string
	ConsumedOffset *uint64
	Replay         bool
	Now            uint64
	Form           InputForm
	Values         [][]byte // Prepared
	TimerKind      TimerKind
	ScheduledAt    uint64
}

// RestoreBody is one host-encoded body, with the version it was written at
// when that is not the running one.
type RestoreBody struct {
	Bytes       []byte
	FromVersion *uint32
}

// RestoreFold is a snapshot, if any, and the change log to fold over it.
type RestoreFold struct {
	Snapshot *RestoreBody
	Changes  []RestoreBody
}

// KeyRequest is the prepare crossing.
type KeyRequest struct {
	Channel       uint32
	Payloads      [][]byte
	WantKeys      bool
	WantEventTime bool
}

// Outcome is an ActivationOutcome variant, in declaration order.
type Outcome uint8

const (
	OutcomeWork Outcome = iota
	OutcomeRestored
	OutcomeSnapshot
	OutcomeStateJSON
	OutcomeStateApplied
	OutcomePrepared
	OutcomeFailed
)

// Result answers one activation.
type Result struct {
	Token   uint64
	Outcome Outcome

	Effects  []ItemEffect     // OutcomeWork
	Bytes    []byte           // OutcomeSnapshot, OutcomeStateJSON
	Prepared []PrepareOutcome // OutcomePrepared
	Error    WorkError        // OutcomeFailed
}

// Verdict is a Verdict variant, in declaration order.
type Verdict uint8

const (
	Ok Verdict = iota
	Retryable
	NonRetryable
	Unreadable
)

// ItemEffect is one processed item's effect record and verdict.
type ItemEffect struct {
	Verdict        Verdict
	Message        string
	Changes        [][]byte
	Outputs        []Output
	ConsumedOffset *uint64
	EventTime      uint64
	Timers         []TimerOp
	Checkpoint     bool
	Complete       []byte // nil when the item completed nothing
}

// Output is one output, already transformed and keyed producer-side.
type Output struct {
	Plugin        bool
	Channel       uint32
	Consumer      uint32
	PluginChannel string
	Key           *string
	EventTime     *uint64
	Metadata      [][2]string
	Payload       []byte
}

// TimerOp is one mutation of the pending-timer set.
type TimerOp struct {
	Kind        TimerKind
	ScheduledAt uint64
	Register    bool
}

// WorkError is a failure the whole activation, or one prepared payload, ended in.
type WorkError struct {
	Message      string
	NonRetryable bool
}

// PrepareOutcome is one payload's prepare answer, or its refusal.
type PrepareOutcome struct {
	Failed    bool
	Error     WorkError
	Values    []PreparedValue
	EventTime *uint64
}

// PreparedValue is one value of a crossed payload for one keyed consumer.
type PreparedValue struct {
	Consumer uint32
	Keys     []string
	Bytes    []byte
}

// DecodeActivation reads one activation.
func DecodeActivation(b []byte) (a Activation, err error) {
	defer recoverDecode(&err)
	r := &reader{buf: b}
	a.Token = r.varint("token")
	a.Relay = r.str("target relay")
	a.Tasklet = r.u32("target tasklet")
	a.CellKey = r.optStr("target cell key")
	a.Kind = Kind(r.variant("activation kind"))
	switch a.Kind {
	case KindWork:
		n := r.count("work items")
		a.Items = make([]WorkItem, 0, n)
		for range n {
			a.Items = append(a.Items, r.workItem())
		}
	case KindRestoreFold:
		if r.some("restore snapshot") {
			body := r.restoreBody()
			a.Restore.Snapshot = &body
		}
		n := r.count("restore changes")
		for range n {
			a.Restore.Changes = append(a.Restore.Changes, r.restoreBody())
		}
	case KindSnapshotRequest, KindLiveStateRead:
	case KindApplyState:
		a.StateJSON = r.bytes("state json")
	case KindKeyRequest:
		a.KeyRequest.Channel = r.u32("key request channel")
		a.KeyRequest.Payloads = r.byteSeq("key request payloads")
		a.KeyRequest.WantKeys = r.bool("want keys")
		a.KeyRequest.WantEventTime = r.bool("want event time")
	case KindCompensateRun:
		a.Outputs = r.byteSeq("compensate outputs")
	case KindLeave:
		a.WholeRelay = r.bool("whole relay")
	default:
		r.fail("activation kind %d is not one this SDK knows", a.Kind)
	}
	a.ChangeSeq = r.varint("change seq")
	a.Factory = r.str("factory")
	if r.some("cell identity") {
		a.CellIdentity = r.bytes("cell identity")
	}
	r.finish("the activation")
	return a, nil
}

// EncodeActivation writes one activation; the core encodes them, and this is
// the inverse the fixture round trip and the test harness use.
func EncodeActivation(a Activation) []byte {
	w := &writer{}
	w.varint(a.Token)
	w.str(a.Relay)
	w.varint(uint64(a.Tasklet))
	w.optStr(a.CellKey)
	w.varint(uint64(a.Kind))
	switch a.Kind {
	case KindWork:
		w.varint(uint64(len(a.Items)))
		for _, item := range a.Items {
			w.workItem(item)
		}
	case KindRestoreFold:
		w.bool(a.Restore.Snapshot != nil)
		if a.Restore.Snapshot != nil {
			w.restoreBody(*a.Restore.Snapshot)
		}
		w.varint(uint64(len(a.Restore.Changes)))
		for _, body := range a.Restore.Changes {
			w.restoreBody(body)
		}
	case KindApplyState:
		w.bytes(a.StateJSON)
	case KindKeyRequest:
		w.varint(uint64(a.KeyRequest.Channel))
		w.byteSeq(a.KeyRequest.Payloads)
		w.bool(a.KeyRequest.WantKeys)
		w.bool(a.KeyRequest.WantEventTime)
	case KindCompensateRun:
		w.byteSeq(a.Outputs)
	case KindLeave:
		w.bool(a.WholeRelay)
	}
	w.varint(a.ChangeSeq)
	w.str(a.Factory)
	w.bool(a.CellIdentity != nil)
	if a.CellIdentity != nil {
		w.bytes(a.CellIdentity)
	}
	return w.buf
}

// TokenOf reads only an activation's token.
func TokenOf(b []byte) (token uint64, err error) {
	defer recoverDecode(&err)
	return (&reader{buf: b}).varint("token"), nil
}

// EncodeResult writes the answer to one activation.
func EncodeResult(res Result) []byte {
	w := &writer{}
	w.varint(res.Token)
	w.varint(uint64(res.Outcome))
	switch res.Outcome {
	case OutcomeWork:
		w.varint(uint64(len(res.Effects)))
		for _, e := range res.Effects {
			w.effect(e)
		}
	case OutcomeSnapshot, OutcomeStateJSON:
		w.bytes(res.Bytes)
	case OutcomePrepared:
		w.varint(uint64(len(res.Prepared)))
		for _, p := range res.Prepared {
			w.prepared(p)
		}
	case OutcomeFailed:
		w.workError(res.Error)
	}
	return w.buf
}

// DecodeResult reads an activation result: the inverse the fixture round trip uses.
func DecodeResult(b []byte) (res Result, err error) {
	defer recoverDecode(&err)
	r := &reader{buf: b}
	res.Token = r.varint("token")
	res.Outcome = Outcome(r.variant("activation outcome"))
	switch res.Outcome {
	case OutcomeWork:
		n := r.count("effects")
		for range n {
			res.Effects = append(res.Effects, r.effect())
		}
	case OutcomeRestored, OutcomeStateApplied:
	case OutcomeSnapshot, OutcomeStateJSON:
		res.Bytes = r.bytes("state")
	case OutcomePrepared:
		n := r.count("prepared")
		for range n {
			res.Prepared = append(res.Prepared, r.prepared())
		}
	case OutcomeFailed:
		res.Error = r.workError()
	default:
		r.fail("activation outcome %d is not one this SDK knows", res.Outcome)
	}
	r.finish("the result")
	return res, nil
}

// DecodeOutput reads one released Output, as local mode hands it out.
func DecodeOutput(b []byte) (o Output, err error) {
	defer recoverDecode(&err)
	r := &reader{buf: b}
	o = r.output()
	r.finish("the output")
	return o, nil
}

// ---------------------------------------------------------------------------
// Decoding.

type decodeError struct{ error }

func recoverDecode(err *error) {
	if r := recover(); r != nil {
		d, ok := r.(decodeError)
		if !ok {
			panic(r)
		}
		*err = d.error
	}
}

// reader walks a postcard buffer; every read names its field, so a malformed
// buffer says where it stopped making sense.
type reader struct {
	buf []byte
	at  int
}

func (r *reader) fail(format string, args ...any) {
	panic(decodeError{fmt.Errorf("flezha: "+format, args...)})
}

func (r *reader) byte(field string) byte {
	if r.at >= len(r.buf) {
		r.fail("%s ran past the end of the buffer", field)
	}
	b := r.buf[r.at]
	r.at++
	return b
}

func (r *reader) varint(field string) uint64 {
	var value uint64
	for shift := uint(0); shift < 70; shift += 7 {
		b := r.byte(field)
		if shift == 63 && b > 1 {
			r.fail("%s is a varint wider than 64 bits", field)
		}
		value |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return value
		}
	}
	r.fail("%s is a varint longer than any 64-bit value can be", field)
	return 0
}

func (r *reader) u32(field string) uint32 {
	v := r.varint(field)
	if v > 0xffffffff {
		r.fail("%s is %d, wider than 32 bits", field, v)
	}
	return uint32(v)
}

func (r *reader) variant(field string) uint64 { return r.varint(field + " variant") }

func (r *reader) count(field string) int {
	n := r.varint(field + " length")
	if n > uint64(len(r.buf)) {
		r.fail("%s claims %d elements, more than the buffer holds", field, n)
	}
	return int(n)
}

func (r *reader) bytes(field string) []byte {
	n := r.varint(field + " length")
	if n > uint64(len(r.buf)-r.at) {
		r.fail("%s claims %d bytes, past the end of the buffer", field, n)
	}
	out := make([]byte, n)
	copy(out, r.buf[r.at:r.at+int(n)])
	r.at += int(n)
	return out
}

func (r *reader) str(field string) string { return string(r.bytes(field)) }

func (r *reader) bool(field string) bool {
	switch b := r.byte(field); b {
	case 0:
		return false
	case 1:
		return true
	default:
		r.fail("%s is %d, which is not a bool", field, b)
		return false
	}
}

func (r *reader) some(field string) bool { return r.bool(field + " presence") }

func (r *reader) optStr(field string) *string {
	if !r.some(field) {
		return nil
	}
	s := r.str(field)
	return &s
}

func (r *reader) optVarint(field string) *uint64 {
	if !r.some(field) {
		return nil
	}
	v := r.varint(field)
	return &v
}

func (r *reader) byteSeq(field string) [][]byte {
	n := r.count(field)
	out := make([][]byte, 0, n)
	for range n {
		out = append(out, r.bytes(field))
	}
	return out
}

func (r *reader) metadata(field string) [][2]string {
	n := r.count(field)
	out := make([][2]string, 0, n)
	for range n {
		out = append(out, [2]string{r.str(field + " key"), r.str(field + " value")})
	}
	return out
}

func (r *reader) finish(what string) {
	if r.at != len(r.buf) {
		r.fail("%s left %d unread byte(s); the buffer does not match the schema this SDK was built against", what, len(r.buf)-r.at)
	}
}

func (r *reader) workItem() WorkItem {
	switch v := r.variant("work item"); v {
	case 0:
		item := WorkItem{
			Channel:        r.u32("input channel"),
			Payload:        r.bytes("input payload"),
			Metadata:       r.metadata("input metadata"),
			ConsumedOffset: r.optVarint("consumed offset"),
			Replay:         r.bool("replay"),
			Now:            r.varint("now"),
			Form:           InputForm(r.variant("input form")),
		}
		switch item.Form {
		case Untransformed, Transformed:
		case Prepared:
			item.Values = r.byteSeq("prepared values")
		default:
			r.fail("an input named form %d, which this SDK does not know", item.Form)
		}
		return item
	case 1:
		kind := TimerKind(r.variant("timer kind"))
		if kind > ProcessingTime {
			r.fail("a timer firing named kind %d, which this SDK does not know", kind)
		}
		return WorkItem{Timer: true, TimerKind: kind, ScheduledAt: r.varint("timer scheduled_at")}
	default:
		r.fail("work item variant %d is not one the schema has", v)
		return WorkItem{}
	}
}

func (r *reader) restoreBody() RestoreBody {
	body := RestoreBody{Bytes: r.bytes("restore body")}
	if r.some("restore body from_version") {
		v := r.u32("restore body from_version")
		body.FromVersion = &v
	}
	return body
}

func (r *reader) output() Output {
	var o Output
	switch v := r.variant("output target"); v {
	case 0:
		o.Channel = r.u32("edge channel")
		o.Consumer = r.u32("edge consumer")
	case 1:
		o.Plugin = true
		o.PluginChannel = r.str("plugin channel")
	default:
		r.fail("output target %d is not one the schema has", v)
	}
	o.Key = r.optStr("output key")
	o.EventTime = r.optVarint("output event time")
	o.Metadata = r.metadata("output metadata")
	o.Payload = r.bytes("output payload")
	return o
}

func (r *reader) effect() ItemEffect {
	var e ItemEffect
	e.Verdict = Verdict(r.variant("verdict"))
	if e.Verdict > Unreadable {
		r.fail("verdict %d is not one the schema has", e.Verdict)
	}
	if e.Verdict != Ok {
		e.Message = r.str("verdict message")
	}
	e.Changes = r.byteSeq("changes")
	n := r.count("outputs")
	for range n {
		e.Outputs = append(e.Outputs, r.output())
	}
	e.ConsumedOffset = r.optVarint("consumed offset")
	e.EventTime = r.varint("event time")
	n = r.count("timers")
	for range n {
		kind := TimerKind(r.variant("timer kind"))
		e.Timers = append(e.Timers, TimerOp{Kind: kind, ScheduledAt: r.varint("scheduled_at"), Register: r.bool("register")})
	}
	e.Checkpoint = r.bool("checkpoint")
	if r.some("dlq") {
		r.fail("a host effect carries no dead-letter entry")
	}
	if r.some("complete") {
		e.Complete = r.bytes("complete")
	}
	return e
}

func (r *reader) workError() WorkError {
	return WorkError{Message: r.str("error message"), NonRetryable: r.bool("non retryable")}
}

func (r *reader) prepared() PrepareOutcome {
	switch v := r.variant("prepare outcome"); v {
	case 0:
		var p PrepareOutcome
		n := r.count("prepared values")
		for range n {
			value := PreparedValue{Consumer: r.u32("consumer")}
			k := r.count("keys")
			for range k {
				value.Keys = append(value.Keys, r.str("key"))
			}
			value.Bytes = r.bytes("value")
			p.Values = append(p.Values, value)
		}
		p.EventTime = r.optVarint("event time")
		return p
	case 1:
		return PrepareOutcome{Failed: true, Error: r.workError()}
	default:
		r.fail("prepare outcome %d is not one the schema has", v)
		return PrepareOutcome{}
	}
}

// ---------------------------------------------------------------------------
// Encoding.

type writer struct{ buf []byte }

func (w *writer) varint(v uint64) {
	for v >= 0x80 {
		w.buf = append(w.buf, byte(v)|0x80)
		v >>= 7
	}
	w.buf = append(w.buf, byte(v))
}

func (w *writer) bytes(b []byte) {
	w.varint(uint64(len(b)))
	w.buf = append(w.buf, b...)
}

func (w *writer) str(s string) {
	w.varint(uint64(len(s)))
	w.buf = append(w.buf, s...)
}

func (w *writer) bool(b bool) {
	if b {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

func (w *writer) optStr(s *string) {
	w.bool(s != nil)
	if s != nil {
		w.str(*s)
	}
}

func (w *writer) optVarint(v *uint64) {
	w.bool(v != nil)
	if v != nil {
		w.varint(*v)
	}
}

func (w *writer) byteSeq(items [][]byte) {
	w.varint(uint64(len(items)))
	for _, b := range items {
		w.bytes(b)
	}
}

func (w *writer) metadata(pairs [][2]string) {
	w.varint(uint64(len(pairs)))
	for _, p := range pairs {
		w.str(p[0])
		w.str(p[1])
	}
}

func (w *writer) workItem(item WorkItem) {
	if item.Timer {
		w.varint(1)
		w.varint(uint64(item.TimerKind))
		w.varint(item.ScheduledAt)
		return
	}
	w.varint(0)
	w.varint(uint64(item.Channel))
	w.bytes(item.Payload)
	w.metadata(item.Metadata)
	w.optVarint(item.ConsumedOffset)
	w.bool(item.Replay)
	w.varint(item.Now)
	w.varint(uint64(item.Form))
	if item.Form == Prepared {
		w.byteSeq(item.Values)
	}
}

func (w *writer) restoreBody(body RestoreBody) {
	w.bytes(body.Bytes)
	w.bool(body.FromVersion != nil)
	if body.FromVersion != nil {
		w.varint(uint64(*body.FromVersion))
	}
}

func (w *writer) output(o Output) {
	if o.Plugin {
		w.varint(1)
		w.str(o.PluginChannel)
	} else {
		w.varint(0)
		w.varint(uint64(o.Channel))
		w.varint(uint64(o.Consumer))
	}
	w.optStr(o.Key)
	w.optVarint(o.EventTime)
	w.metadata(o.Metadata)
	w.bytes(o.Payload)
}

func (w *writer) effect(e ItemEffect) {
	w.varint(uint64(e.Verdict))
	if e.Verdict != Ok {
		w.str(e.Message)
	}
	w.byteSeq(e.Changes)
	w.varint(uint64(len(e.Outputs)))
	for _, o := range e.Outputs {
		w.output(o)
	}
	w.optVarint(e.ConsumedOffset)
	w.varint(e.EventTime)
	w.varint(uint64(len(e.Timers)))
	for _, t := range e.Timers {
		w.varint(uint64(t.Kind))
		w.varint(t.ScheduledAt)
		w.bool(t.Register)
	}
	w.bool(e.Checkpoint)
	// The core mints the dead-letter entry when its own ladder parks.
	w.bool(false)
	w.bool(e.Complete != nil)
	if e.Complete != nil {
		w.bytes(e.Complete)
	}
}

func (w *writer) workError(e WorkError) {
	w.str(e.Message)
	w.bool(e.NonRetryable)
}

func (w *writer) prepared(p PrepareOutcome) {
	if p.Failed {
		w.varint(1)
		w.workError(p.Error)
		return
	}
	w.varint(0)
	w.varint(uint64(len(p.Values)))
	for _, v := range p.Values {
		w.varint(uint64(v.Consumer))
		w.varint(uint64(len(v.Keys)))
		for _, k := range v.Keys {
			w.str(k)
		}
		w.bytes(v.Bytes)
	}
	w.optVarint(p.EventTime)
}
