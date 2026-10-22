package flezha

import "reflect"

// ShortOptions are a short form's channels and class settings: the entry
// channel ("input" when empty) and its codec, the plugin output the tasklet's
// output reaches (none when empty), and the RelaySpec settings.
type ShortOptions struct {
	Input         string
	Codec         ChannelCodec
	Output        string
	Run           any
	Durable       any
	MaxDurationMs int64
	Description   string
}

// ShortRelay declares a one-tasklet relay from a function: it emits the value
// fn returns when the boolean is true, and nothing when it is false. The
// tasklet is named factoryID; a stateless one uses NoState.
func ShortRelay[I, S, O any](factoryID string, fn func(in I, ctx *Ctx[S, O]) (O, bool), opts ...ShortOptions) Relay {
	return shortRelay[I, S, O]{factoryID: factoryID, tasklet: shortFunc[I, S, O]{fn: fn}, opts: first(opts)}
}

// ShortTasklet declares a one-tasklet relay from a tasklet type, whose hooks
// it keeps. The tasklet is named factoryID: its Spec leaves Name empty or
// writes the same id.
func ShortTasklet[I, S, O any](factoryID string, t Tasklet[I, S, O], opts ...ShortOptions) Relay {
	return shortRelay[I, S, O]{factoryID: factoryID, tasklet: t, opts: first(opts)}
}

type shortRelay[I, S, O any] struct {
	factoryID string
	tasklet   Tasklet[I, S, O]
	opts      ShortOptions
}

func (s shortRelay[I, S, O]) Spec() RelaySpec {
	return RelaySpec{FactoryID: s.factoryID, Run: s.opts.Run, Durable: s.opts.Durable,
		MaxDurationMs: s.opts.MaxDurationMs, Description: s.opts.Description}
}

func (s shortRelay[I, S, O]) Channels(c *ChannelBuilder) {
	spec := s.tasklet.Spec()
	switch {
	case spec.Name != "" && spec.Name != s.factoryID:
		c.fail(refusal("topology.short_form_name_mismatch", map[string]any{
			"class": typeName(reflect.TypeOf(s.tasklet)), "factory_id": s.factoryID, "name": spec.Name,
		}))
	case hasTopology(s.tasklet):
		c.fail(refusal("topology.short_form_with_topology", map[string]any{"factory_id": s.factoryID}))
	case s.opts.Output != "" && typeOf[O]() == typeOf[NoOutput]():
		c.fail(refusal("topology.output_without_type", map[string]any{"factory_id": s.factoryID}))
	}
	input := s.opts.Input
	if input == "" {
		input = "input"
	}
	in := Input[I](c, input, InputOptions{Codec: s.opts.Codec})
	node := c.nodeFor(defineAs(s.tasklet, spec, s.factoryID), "")
	out := wire[I, O](in, node, map[string]any{"link": "into", "tasklet": node.index})
	if s.opts.Output != "" {
		out.Output(s.opts.Output)
	}
}

// hasTopology answers whether a short form's tasklet also carries a channels body.
func hasTopology(t any) bool {
	_, ok := t.(interface{ Channels(*ChannelBuilder) })
	return ok
}

// shortFunc is a function short form as a tasklet.
type shortFunc[I, S, O any] struct {
	fn func(in I, ctx *Ctx[S, O]) (O, bool)
}

func (shortFunc[I, S, O]) Spec() Spec { return Spec{} }

func (f shortFunc[I, S, O]) Run(in I, ctx *Ctx[S, O]) error {
	if out, ok := f.fn(in, ctx); ok {
		return ctx.Emit(out)
	}
	return nil
}

func first[T any](opts []T) T {
	var zero T
	if len(opts) > 0 {
		return opts[0]
	}
	return zero
}
