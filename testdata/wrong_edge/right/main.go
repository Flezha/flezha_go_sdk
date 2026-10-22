// The into fixture with its edge right: it compiles.
package main

import flezha "github.com/flezha/flezha_go_sdk"

type Order struct{ Qty int }

type Totals struct{}

func (Totals) Spec() flezha.Spec { return flezha.Spec{Name: "totals"} }

func (Totals) Run(in Order, ctx *flezha.Ctx[flezha.NoState, int]) error { return ctx.Emit(in.Qty) }

func channels(c *flezha.ChannelBuilder) {
	flezha.Into(flezha.Input[Order](c, "orders"), Totals{})
}

func main() { _ = channels }
