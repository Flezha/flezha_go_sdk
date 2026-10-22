package flezha

import (
	"fmt"
	"reflect"
)

// ChannelBuilder is what a relay's Channels receives: entry channels come from
// Input, tasklets are wired with Into, and every step that changes the element
// type is a package function taking and returning typed streams, so a
// mismatched edge fails `go build`.
//
// The builder records the chains as written and judges nothing: the core
// checks the record when the relay registers, and its refusal is the error
// RegisterFactory returns.
type ChannelBuilder struct {
	factoryID   string
	tasklets    []*taskNode
	channels    []*channelSpec
	chains      []chainRecord
	sealed      bool
	mergeGroups int
	err         error
}

// chainRecord is one fluent sequence off a channel handle; only a tip whose
// prefix no later link built on reaches the record.
type chainRecord struct {
	source     source
	links      []map[string]any
	superseded bool
	edge       *consumerEdge
}

// source is where a stream's values leave from: an entry channel or a node.
type source struct {
	entry *channelSpec
	node  *taskNode
}

// taskNode is a tasklet as the graph holds it.
type taskNode struct {
	index  int
	name   string
	def    *definition
	output *channelSpec
}

// channelSpec is a channel as the graph holds it; its position is its id.
type channelSpec struct {
	index          int
	name           string
	kind           string
	producer       *int
	chains         [][]map[string]any
	chainEdges     []*consumerEdge
	consumers      []*consumerEdge
	derivedKind    string
	overflow       Overflow
	retryMax       *int
	retryBackoffMs *int
	keyed          bool
	sink           *string
	sinkEncode     func(any) ([]byte, error)
	codec          string
	decode         func([]byte) (any, error)
	call           *callEnd
	schema         any
	description    string
	valueType      reflect.Type
}

// consumerEdge is one consumer of a channel, with its own edge's transforms
// and key: a chain declared before one Into reaches no other.
type consumerEdge struct {
	tasklet    int
	transforms []func(any) []any
	keyBy      func(any) string
}

// callEnd is a channel's call end: the transforms and key of the chain that
// closed on it, and the request's encoder.
type callEnd struct {
	transforms []func(any) []any
	keyBy      func(any) string
	encode     func(any) ([]byte, error)
}

// Overflow is what a channel does when its consumer's mailbox is full.
type Overflow string

const (
	DropNothing Overflow = "drop_nothing"
	DropNewest  Overflow = "drop_newest"
	DropOldest  Overflow = "drop_oldest"
)

// ChannelCodec is the payload format a channel names. A channel that names
// none reads JSON, and an output that names none sends a string or a []byte
// as it stands and anything else as JSON.
type ChannelCodec string

const (
	JSON  ChannelCodec = "json"
	Text  ChannelCodec = "text"
	Bytes ChannelCodec = "bytes"
)

func newBuilder(factoryID string) *ChannelBuilder {
	return &ChannelBuilder{factoryID: factoryID}
}

// fail keeps the first refusal a declaration meets; the builder goes on
// recording, and registration returns it.
func (c *ChannelBuilder) fail(err error) {
	if c.err == nil {
		c.err = err
	}
}

// InputOptions are an entry channel's optional settings.
type InputOptions struct {
	// The payload format: JSON by default; Text needs a string stream and
	// Bytes a []byte one.
	Codec ChannelCodec
	// What the channel is for, in the relay's API.
	Description string
	// The channel's JSON Schema, as a value json.Marshal writes as an object;
	// nil derives one from T when T is a struct and the codec is JSON.
	Schema any
}

// Input declares a controller-fed entry channel whose payloads decode into T.
func Input[T any](c *ChannelBuilder, name string, opts ...InputOptions) Stream[T] {
	o := first(opts)
	codec := o.Codec
	if codec == "" {
		codec = JSON
	}
	decode, err := decoderFor[T](codec, name)
	if err != nil {
		c.fail(err)
	}
	ch := c.entry(name, string(codec), decode)
	ch.description, ch.schema = o.Description, o.Schema
	ch.valueType = typeOf[T]()
	return Stream[T]{b: c, from: source{entry: ch}}
}

// InputDecoded declares an entry channel whose payloads decode is the
// author's own.
func InputDecoded[T any](c *ChannelBuilder, name string, decode func([]byte) (T, error), opts ...InputOptions) Stream[T] {
	o := first(opts)
	if o.Codec != "" {
		c.fail(refusal("topology.codec_with_callable", map[string]any{"channel": name}))
	}
	ch := c.entry(name, "custom", func(b []byte) (any, error) { return decode(b) })
	ch.description, ch.schema = o.Description, o.Schema
	return Stream[T]{b: c, from: source{entry: ch}}
}

func (c *ChannelBuilder) entry(name, codec string, decode func([]byte) (any, error)) *channelSpec {
	ch := &channelSpec{index: len(c.channels), name: name, kind: "entry", codec: codec, decode: decode, overflow: DropNothing}
	c.channels = append(c.channels, ch)
	return ch
}

// nodeFor is the node for a tasklet: two unnamed mentions of one tasklet type
// are one node, so a diamond has one instance; another type under the same
// identity is the core's node_identity_collision.
func (c *ChannelBuilder) nodeFor(def *definition, name string) *taskNode {
	if def.err != nil {
		c.fail(def.err)
	}
	identity := name
	if identity == "" {
		identity = def.name
	}
	for _, node := range c.tasklets {
		if node.name != identity {
			continue
		}
		if node.def.typ == def.typ {
			return node
		}
		c.fail(refusal("topology.node_identity_collision", map[string]any{"name": identity}))
		return node
	}
	node := &taskNode{index: len(c.tasklets), name: identity, def: def}
	c.tasklets = append(c.tasklets, node)
	return node
}

// outputChannelFor is the one channel a node produces on, created on first use.
func (c *ChannelBuilder) outputChannelFor(node *taskNode) *channelSpec {
	if node.output != nil {
		return node.output
	}
	producer := node.index
	ch := &channelSpec{
		index: len(c.channels), name: node.name, kind: "inter_task", producer: &producer,
		overflow: DropNothing, decode: node.def.decodeOut, valueType: node.def.outType,
	}
	c.channels = append(c.channels, ch)
	node.output = ch
	return ch
}

// linkChain records one more link on the sequence at base, or opens one on
// src, and answers the new sequence's index. The links are copied: a handle
// held over a branch point extends its prefix twice.
func (c *ChannelBuilder) linkChain(src source, base *int, link map[string]any, edge *consumerEdge) int {
	var links []map[string]any
	if base != nil {
		links = append(links, c.chains[*base].links...)
		c.chains[*base].superseded = true
	}
	links = append(links, link)
	c.chains = append(c.chains, chainRecord{source: src, links: links, edge: edge})
	return len(c.chains) - 1
}

// seal files every sequence on the channel it was declared off.
func (c *ChannelBuilder) seal() {
	if c.sealed {
		return
	}
	c.sealed = true
	for _, chain := range c.chains {
		if chain.superseded {
			continue
		}
		ch := chain.source.entry
		if ch == nil {
			ch = c.outputChannelFor(chain.source.node)
		}
		ch.chains = append(ch.chains, chain.links)
		ch.chainEdges = append(ch.chainEdges, chain.edge)
	}
}

// Stream is one edge under construction, carrying values of type T. It is a
// value: every call answers a new stream, so a shared prefix can branch
// without its arms colliding.
type Stream[T any] struct {
	b       *ChannelBuilder
	from    source
	pending pending
}

type pending struct {
	transforms     []func(any) []any
	keyBy          func(any) string
	chain          *int
	channelName    *string
	overflow       *Overflow
	retryMax       *int
	retryBackoffMs *int
	keyed          bool
}

// Named overrides the channel name this edge creates.
func (s Stream[T]) Named(name string) Stream[T] {
	return s.with(func(p *pending) { p.channelName = &name })
}

// Filter drops the values pred rejects; a dropped value is consumed and
// reaches no handler.
func (s Stream[T]) Filter(pred func(T) bool) Stream[T] {
	return s.relinkTransform(func(v any) []any {
		if pred(as[T](v)) {
			return []any{v}
		}
		return nil
	})
}

// KeyBy routes by a computed key: the consumer becomes a dynamic keyed tasklet.
func (s Stream[T]) KeyBy(key func(T) string) Stream[T] {
	return s.relink("key", func(p *pending) { p.keyBy = func(v any) string { return key(as[T](v)) } })
}

// OnFull sets what the channel does when its consumer's mailbox is full.
func (s Stream[T]) OnFull(policy Overflow) Stream[T] {
	return s.with(func(p *pending) { p.overflow = &policy })
}

// RetryMax is the channel's attempt ceiling.
func (s Stream[T]) RetryMax(attempts int) Stream[T] {
	return s.with(func(p *pending) { p.retryMax = &attempts })
}

// RetryBackoffMs is the channel's backoff between attempts.
func (s Stream[T]) RetryBackoffMs(ms int) Stream[T] {
	return s.with(func(p *pending) { p.retryBackoffMs = &ms })
}

// Keyed stamps `flz.cell` / `flz.seq` / `flz.key` onto this channel's plugin frames.
func (s Stream[T]) Keyed() Stream[T] {
	return s.with(func(p *pending) { p.keyed = true })
}

// Branch fans out: each arm receives this same edge, so Branch builds exactly
// the topology the separate statements would.
func (s Stream[T]) Branch(arms ...func(Stream[T])) Stream[T] {
	s.b.linkChain(s.from, nil, map[string]any{"link": "branch", "arms": len(arms)}, nil)
	for _, arm := range arms {
		arm(s)
	}
	return s
}

// Output terminates this edge's channel on the plugin layer. A name written
// here is the channel's name; "" leaves it derived. Without a codec a string
// or []byte is sent as it stands and anything else as JSON.
func (s Stream[T]) Output(name string, codec ...ChannelCodec) Stream[T] {
	if len(codec) > 0 {
		return s.attachSink(name, nil, codec[0])
	}
	return s.attachSink(name, nil, "")
}

// OutputEncoded is Output with the plugin frame's bytes written by encode.
func (s Stream[T]) OutputEncoded(name string, encode func(T) ([]byte, error)) Stream[T] {
	return s.attachSink(name, func(v any) ([]byte, error) { return encode(as[T](v)) }, "")
}

// attachSink ends the channel in an output: encode is the author's own, codec
// a named format, and with neither a value is sent as plain does.
func (s Stream[T]) attachSink(name string, encode func(any) ([]byte, error), codec ChannelCodec) Stream[T] {
	if s.from.node == nil {
		derived := name
		if derived == "" {
			derived = "<derived>"
		}
		s.b.fail(refusal("topology.output_on_entry", map[string]any{"channel": derived}))
		return s
	}
	ch := s.channel()
	if name != "" {
		ch.name = name
	}
	link := map[string]any{"link": "sink"}
	if name != "" {
		link["name"] = name
	}
	s.b.linkChain(s.from, s.pending.chain, link, nil)
	if ch.sink == nil {
		sink := ch.name
		ch.sink = &sink
		switch {
		case encode != nil:
			ch.sinkEncode, ch.codec = encode, "custom"
		case codec != "":
			ch.sinkEncode, ch.codec = encoderFor(codec, ch), string(codec)
		default:
			ch.sinkEncode = func(v any) ([]byte, error) { return plain(v, "channel '"+ch.name+"'") }
			ch.codec = string(JSON)
		}
	}
	return s
}

// channel is the channel this edge's values leave on, with the pending
// options applied.
func (s Stream[T]) channel() *channelSpec {
	var ch *channelSpec
	if s.from.entry != nil {
		ch = s.from.entry
	} else {
		ch = s.b.outputChannelFor(s.from.node)
		if rename := s.pending.channelName; rename != nil && ch.name != *rename {
			ch.name = *rename
			s.b.linkChain(s.from, nil, map[string]any{"link": "named", "name": *rename}, nil)
		}
	}
	if s.pending.overflow != nil {
		ch.overflow = *s.pending.overflow
	}
	if s.pending.retryMax != nil {
		ch.retryMax = s.pending.retryMax
	}
	if s.pending.retryBackoffMs != nil {
		ch.retryBackoffMs = s.pending.retryBackoffMs
	}
	ch.keyed = ch.keyed || s.pending.keyed
	return ch
}

func (s Stream[T]) copy() Stream[T] {
	next := s
	next.pending.transforms = append([]func(any) []any(nil), s.pending.transforms...)
	return next
}

// with applies an option now, as the reference does: one no terminator
// follows would otherwise be dropped.
func (s Stream[T]) with(set func(*pending)) Stream[T] {
	next := s.copy()
	set(&next.pending)
	next.channel()
	return next
}

func (s Stream[T]) relinkTransform(transform func(any) []any) Stream[T] {
	return s.relink("transform", func(p *pending) { p.transforms = append(p.transforms, transform) })
}

// relink is this edge with one more chain link: the one place a transform's
// or a key's edge is built, so every link reaches the record.
func (s Stream[T]) relink(link string, set func(*pending)) Stream[T] {
	next := s.copy()
	set(&next.pending)
	index := s.b.linkChain(s.from, s.pending.chain, map[string]any{"link": link}, nil)
	next.pending.chain = &index
	return next
}

// wire closes this edge's chain on node with link, and answers the node's edge.
func wire[T, O any](s Stream[T], node *taskNode, link map[string]any) Stream[O] {
	s.channel()
	s.b.linkChain(s.from, s.pending.chain, link, &consumerEdge{
		tasklet: node.index, transforms: s.pending.transforms, keyBy: s.pending.keyBy,
	})
	return Stream[O]{b: s.b, from: source{node: node}}
}

// Map transforms every value on the edge, producer-side.
func Map[T, U any](s Stream[T], f func(T) U) Stream[U] {
	next := s.relinkTransform(func(v any) []any { return []any{f(as[T](v))} })
	return Stream[U]{b: next.b, from: next.from, pending: next.pending}
}

// FlatMap transforms one value into many; each reaches the consumer on its own.
func FlatMap[T, U any](s Stream[T], f func(T) []U) Stream[U] {
	next := s.relinkTransform(func(v any) []any {
		out := f(as[T](v))
		values := make([]any, len(out))
		for i, u := range out {
			values[i] = u
		}
		return values
	})
	return Stream[U]{b: next.b, from: next.from, pending: next.pending}
}

// Into wires the stream into a tasklet whose input is the stream's element
// type, and answers the tasklet's output stream.
func Into[I, S, O any](s Stream[I], t Tasklet[I, S, O]) Stream[O] {
	return IntoNamed(s, t, "")
}

// IntoNamed is Into under an explicit node identity, for a second instance of
// one tasklet type.
func IntoNamed[I, S, O any](s Stream[I], t Tasklet[I, S, O], nodeName string) Stream[O] {
	node := s.b.nodeFor(define(t), nodeName)
	return wire[I, O](s, node, map[string]any{"link": "into", "tasklet": node.index})
}

// Merge merges several channels into one tasklet, each source lifted into the
// merged value M by its own mapping.
type Merge[M any] struct {
	b       *ChannelBuilder
	sources []Stream[M]
	keys    []func(M) string
}

// NewMerge starts a merge on c.
func NewMerge[M any](c *ChannelBuilder) *Merge[M] { return &Merge[M]{b: c} }

// From adds one source; its mapping is an ordinary map on the source's chain.
func From[T, M any](m *Merge[M], s Stream[T], lift func(T) M) *Merge[M] {
	m.sources = append(m.sources, Map(s, lift))
	return m
}

// KeyBy routes every source by a key computed on the merged value.
func (m *Merge[M]) KeyBy(key func(M) string) *Merge[M] {
	m.keys = append(m.keys, key)
	return m
}

// MergeInto closes every source on one tasklet node, and answers its output.
func MergeInto[S, O, M any](m *Merge[M], t Tasklet[M, S, O]) Stream[O] {
	return MergeIntoNamed(m, t, "")
}

// MergeIntoNamed is MergeInto under an explicit node identity.
func MergeIntoNamed[S, O, M any](m *Merge[M], t Tasklet[M, S, O], nodeName string) Stream[O] {
	node := m.b.nodeFor(define(t), nodeName)
	link := map[string]any{"link": "merge", "tasklet": node.index, "group": m.b.mergeGroups}
	m.b.mergeGroups++
	for _, src := range m.sources {
		for _, key := range m.keys {
			src = src.KeyBy(key)
		}
		wire[M, O](src, node, link)
	}
	return Stream[O]{b: m.b, from: source{node: node}}
}

// as is v as T: a value an edge carries is the stream's type by construction,
// and nil is T's zero value.
func as[T any](v any) T {
	t, _ := v.(T)
	return t
}

func typeOf[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

// typeName is a Go type as a refusal names it.
func typeName(t reflect.Type) string {
	if t == nil {
		return "nil"
	}
	return fmt.Sprint(t)
}
