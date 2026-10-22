// The echo relay: each message on "in" leaves on "out" behind "echo: ".
//
//	go run ./examples/echo                 # a runner on namespace "default"
//	flz relay start echo --id my_echo
//	flz relay send my_echo --text 'hello world' --await 2000
package main

import (
	"log"

	flezha "github.com/flezha/flezha_go_sdk"
)

// Echo is a processing stage of the relay's graph, run once per message. It
// keeps no state and emits through its context, the one way a value leaves a
// tasklet; where the value goes is the relay's to decide.
type Echo struct{}

func (Echo) Spec() flezha.Spec { return flezha.Spec{Name: "echo"} }

func (Echo) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit("echo: " + in)
}

// EchoRelay is the deployable unit: the entry channel a message arrives on, the
// tasklet it passes through and the channel its result leaves on. The entry
// reads text, so the tasklet is handed a string.
type EchoRelay struct{}

func (EchoRelay) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "echo"} }

func (EchoRelay) Channels(c *flezha.ChannelBuilder) {
	in := flezha.Input[string](c, "in", flezha.InputOptions{Codec: flezha.Text})
	flezha.Into(in, Echo{}).Output("out")
}

func main() {
	runner, err := flezha.CreateRunner("default")
	if err != nil {
		log.Fatal(err)
	}
	if err := runner.RegisterFactory(EchoRelay{}); err != nil {
		log.Fatal(err)
	}
	if err := runner.Run(); err != nil {
		log.Fatal(err)
	}
}
