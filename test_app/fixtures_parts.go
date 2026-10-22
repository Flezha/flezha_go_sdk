// Twins of the reference's part fixtures (test/apps/rust_runner/src/part.rs):
// for an input `n` the tasklet sends the parts "1" .. "n" and then emits
// "done:<n>" on its output `answer`. `retry:<n>` sends its parts and then fails
// retryably the first time this process sees it, so the retry's parts number
// from 0 again under the next attempt.

package main

import (
	"errors"
	"strconv"
	"strings"
	"sync"

	flezha "github.com/flezha/flezha_go_sdk"
)

// failedOnceCore is the `retry:` inputs that have already failed once in this process.
var failedOnceCore sync.Map

type partSenderCore struct{ name string }

func (t partSenderCore) Spec() flezha.Spec { return flezha.Spec{Name: t.name} }

func (partSenderCore) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	count, retry := strings.CutPrefix(in, "retry:")
	if count == "" || strings.Trim(count, "0123456789") != "" {
		return flezha.NonRetryable(errors.New("not a part count: " + in))
	}
	n, err := strconv.Atoi(count)
	if err != nil {
		return flezha.NonRetryable(errors.New("not a part count: " + in))
	}
	for part := 1; part <= n; part++ {
		if err := ctx.Part(strconv.Itoa(part)); err != nil {
			return err
		}
	}
	if retry {
		if _, failed := failedOnceCore.LoadOrStore(in, true); !failed {
			return errors.New("first attempt of " + in)
		}
	}
	return ctx.Emit("done:" + strconv.Itoa(n))
}

func partEchoRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "part_echo"}, func(c *flezha.ChannelBuilder) {
		in := textIn(c, "in").RetryBackoffMs(25)
		flezha.Into(in, partSenderCore{name: "part_echo"}).Output("answer")
	}}
}

func partEchoStepsRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{
		FactoryID:     "part_echo_steps",
		Run:           map[string]any{"steps": map[string]any{"each": "per_input"}},
		Durable:       fixtureDurable(),
		MaxDurationMs: 600_000,
	}, func(c *flezha.ChannelBuilder) {
		in := textIn(c, "in").RetryBackoffMs(25)
		flezha.Into(in, partSenderCore{name: "part_echo_turn"}).Output("answer")
	}}
}

func partFixtures() []flezha.Relay {
	return []flezha.Relay{partEchoRelay(), partEchoStepsRelay()}
}
