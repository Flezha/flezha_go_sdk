// The probe family, poison_probe and wedge_probe: the same partition as the
// reference's probe.rs.

package main

import (
	"errors"
	"sync/atomic"
	"time"

	flezha "github.com/flezha/flezha_go_sdk"
)

// -- poison_probe ------------------------------------------------------------
//
// `run` fails on the input "poison" until an "antidote" input flips a
// process-wide flag.

var antidoteTakenCore atomic.Bool

type poisonProbeCore struct{}

func (poisonProbeCore) Spec() flezha.Spec { return flezha.Spec{Name: "poison_probe_tasklet"} }

func (poisonProbeCore) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	if in == "antidote" {
		antidoteTakenCore.Store(true)
	}
	if in == "poison" && !antidoteTakenCore.Load() {
		return errors.New("poison payload")
	}
	return ctx.Emit("ok:" + in)
}

func poisonProbeRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "poison_probe"}, func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "input").RetryBackoffMs(25), poisonProbeCore{}).Output("out")
	}}
}

// -- wedge_probe -------------------------------------------------------------
//
// The `pool_wedge` fixture: the ways a runner stops doing work, on one relay.
//
//	"wedge" blocks the dispatch inside user code forever, so the dispatch
//	  counter stops advancing while the heartbeat keeps reporting the runner
//	  alive.
//	"abort" ends its activation as violently as this host allows, without
//	  emitting, once per process: a panic, which the dispatcher recovers and
//	  retries, so the cell is served again and the item produces once.
//	"slow" emits two seconds late, so the relay can be stopped or moved while
//	  the activation still runs.

var wedgeAbortedCore atomic.Bool

type wedgeProbeCore struct{}

func (wedgeProbeCore) Spec() flezha.Spec { return flezha.Spec{Name: "wedge_probe_tasklet"} }

func (wedgeProbeCore) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	if in == "wedge" {
		for {
			time.Sleep(time.Hour)
		}
	}
	if in == "abort" && !wedgeAbortedCore.Swap(true) {
		panic("wedge_probe: killing the worker holding this activation")
	}
	if in == "slow" {
		time.Sleep(2 * time.Second)
	}
	return ctx.Emit("ok:" + in)
}

func wedgeProbeRelay() flezha.Relay {
	return fixtureRelay{flezha.RelaySpec{FactoryID: "wedge_probe"}, func(c *flezha.ChannelBuilder) {
		flezha.Into(textIn(c, "input"), wedgeProbeCore{}).Output("out")
	}}
}

func probeFixtures() []flezha.Relay {
	return []flezha.Relay{poisonProbeRelay(), wedgeProbeRelay()}
}
