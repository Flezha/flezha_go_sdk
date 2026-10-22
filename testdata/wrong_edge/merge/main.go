// A merge lifting into Quote closed on a tasklet whose input is Order.
package main

import flezha "github.com/flezha/flezha_go_sdk"

type Order struct{ Qty int }

type Quote struct{ Price float64 }

type Totals struct{}

func (Totals) Spec() flezha.Spec { return flezha.Spec{Name: "totals"} }

func (Totals) Run(in Order, ctx *flezha.Ctx[flezha.NoState, int]) error { return ctx.Emit(in.Qty) }

func channels(c *flezha.ChannelBuilder) {
	m := flezha.NewMerge[Quote](c)
	flezha.From(m, flezha.Input[float64](c, "prices"), func(p float64) Quote { return Quote{Price: p} })
	flezha.MergeInto(m, Totals{})
}

func main() { _ = channels }
