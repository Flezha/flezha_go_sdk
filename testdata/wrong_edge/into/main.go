// A Stream[string] wired into a tasklet whose input is Order.
package main

import flezha "github.com/flezha/flezha_go_sdk"

type Order struct{ Qty int }

type Totals struct{}

func (Totals) Spec() flezha.Spec { return flezha.Spec{Name: "totals"} }

func (Totals) Run(in Order, ctx *flezha.Ctx[flezha.NoState, int]) error { return ctx.Emit(in.Qty) }

func channels(c *flezha.ChannelBuilder) {
	flezha.Into(flezha.Input[string](c, "orders"), Totals{})
}

func main() { _ = channels }
