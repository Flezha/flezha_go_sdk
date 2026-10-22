// Package flezha is the Flezha Go SDK: a poll host over the shared core,
// linked in through its C ABI.
//
// The SDK holds no kernel code. Transport, framing, ordering, per-cell
// exclusivity, durability, retries, timers, routing, the steps driver and
// reconnects are the core's; this package brings the authoring surface, the
// codec for Go types, the goroutine host and idiomatic errors.
//
//	type Echo struct{}
//
//	func (Echo) Spec() flezha.Spec { return flezha.Spec{Name: "echo"} }
//
//	func (Echo) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
//		return ctx.Emit(in)
//	}
//
//	type EchoRelay struct{}
//
//	func (EchoRelay) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "echo"} }
//
//	func (EchoRelay) Channels(c *flezha.ChannelBuilder) {
//		flezha.Into(flezha.Input[string](c, "input"), Echo{}).Output("out")
//	}
//
//	runner, err := flezha.CreateRunner("default")
//	...
//	err = runner.RegisterFactory(EchoRelay{})
//	err = runner.Run()
package flezha
