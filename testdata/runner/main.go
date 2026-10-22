// A runner the SDK's runner tests launch against the fake controller. The
// relay it serves is the first argument:
//
//	echo    a text echo into the output "out"
//	slow    keyed by input, each input held SLOW_MS (1000) before it echoes
//	wedge   a producer emitting five values per input into a consumer whose
//	        mailbox holds one and drops nothing, which echoes each
//
//	none    no relay type at all
//
// It prints "started" once Start returned, and "handling <input>" as a slow
// input enters its handler. With NOTICE_LOG set, its logger writes there and
// nothing else, and a Start that fails exits 0. With RETRY_START set it starts
// twice and prints each refusal's code. With CLOSE_AFTER_MS set it closes
// itself that long after starting, and prints "closed" once
// AwaitTermination returned.
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	flezha "github.com/flezha/flezha_go_sdk"
)

type echo struct{ name string }

func (e echo) Spec() flezha.Spec { return flezha.Spec{Name: e.name} }

func (echo) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error { return ctx.Emit(in) }

type slow struct{}

func (slow) Spec() flezha.Spec { return flezha.Spec{Name: "slow"} }

func (slow) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	fmt.Fprintln(os.Stderr, "handling", in)
	ms, err := strconv.Atoi(os.Getenv("SLOW_MS"))
	if err != nil {
		ms = 1000
	}
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return ctx.Emit(in)
}

type fanOut struct{}

func (fanOut) Spec() flezha.Spec { return flezha.Spec{Name: "fan_out"} }

func (fanOut) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	for i := range 5 {
		if err := ctx.Emit(in + "-" + strconv.Itoa(i)); err != nil {
			return err
		}
	}
	return nil
}

type narrow struct{}

func (narrow) Spec() flezha.Spec {
	one := 1
	return flezha.Spec{Name: "narrow", MailboxCapacity: &one}
}

func (narrow) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	time.Sleep(20 * time.Millisecond)
	return ctx.Emit(in)
}

type relay struct{ name string }

func (r relay) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: r.name} }

func (r relay) Channels(c *flezha.ChannelBuilder) {
	in := flezha.Input[string](c, "input", flezha.InputOptions{Codec: flezha.Text})
	switch r.name {
	case "echo":
		flezha.Into(in, echo{"echo"}).Output("out")
	case "slow":
		flezha.Into(in.KeyBy(func(s string) string { return s }), slow{}).Output("out")
	case "wedge":
		flezha.Into(flezha.Into(in, fanOut{}).OnFull(flezha.DropNothing), narrow{}).Output("out")
	}
}

func main() {
	logTo := os.Stderr
	if path := os.Getenv("NOTICE_LOG"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		logTo = f
	}
	runner, err := flezha.CreateRunner("go_sdk_test", flezha.WithLogger(slog.New(slog.NewTextHandler(logTo, nil))))
	if err == nil && os.Args[1] != "none" {
		err = runner.RegisterFactory(relay{os.Args[1]})
	}
	if err == nil && os.Getenv("RETRY_START") != "" {
		for range 2 {
			var refused *flezha.Error
			if errors.As(runner.Start(), &refused) {
				fmt.Fprintln(os.Stderr, "start refused:", refused.Code)
			}
		}
		os.Exit(0)
	}
	if err == nil {
		err = runner.Start()
	}
	if err != nil && logTo != os.Stderr {
		os.Exit(0)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	fmt.Fprintln(os.Stderr, "started")
	if ms, _ := strconv.Atoi(os.Getenv("CLOSE_AFTER_MS")); ms > 0 {
		time.AfterFunc(time.Duration(ms)*time.Millisecond, runner.Close)
	}
	if err := runner.AwaitTermination(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	fmt.Fprintln(os.Stderr, "closed")
}
