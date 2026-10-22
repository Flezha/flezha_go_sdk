package main

import (
	"os"
	"regexp"
	"strconv"
	"strings"

	flezha "github.com/flezha/flezha_go_sdk"
)

// prefix is the key convention every keyed fixture shares: text before the first ':'.
func prefix(input string) string {
	head, _, _ := strings.Cut(input, ":")
	return head
}

var integer = regexp.MustCompile(`^-?\d+$`)

// wireInt is an entry read as the reference's i64: an unparsable input reads 0.
func wireInt(input string) int64 {
	if !integer.MatchString(input) {
		return 0
	}
	n, _ := strconv.ParseInt(input, 10, 64)
	return n
}

// fixtureDurable is a kept instance that sleeps after
// FLEZHA_FIXTURE_SLEEP_AFTER_MS, so a test can watch one sleep; "local" when unset.
func fixtureDurable() any {
	if ms := wireInt(os.Getenv("FLEZHA_FIXTURE_SLEEP_AFTER_MS")); ms > 0 {
		return map[string]any{"local": map[string]any{"sleep_after": ms}}
	}
	return "local"
}

// benchCheckpoint is bench_checkpoint's acknowledgement: every
// FLEZHA_BENCH_CHECKPOINT_EVERY inputs (500), and with
// FLEZHA_BENCH_CHECKPOINT_OUTPUTS=at_checkpoint its outputs held until the
// interval closes, for at most FLEZHA_BENCH_CHECKPOINT_DELAY_MS (50).
func benchCheckpoint() map[string]any {
	every := wireInt(os.Getenv("FLEZHA_BENCH_CHECKPOINT_EVERY"))
	if every == 0 {
		every = 500
	}
	checkpoint := map[string]any{"every": every}
	if os.Getenv("FLEZHA_BENCH_CHECKPOINT_OUTPUTS") == "at_checkpoint" {
		delay := wireInt(os.Getenv("FLEZHA_BENCH_CHECKPOINT_DELAY_MS"))
		if delay == 0 {
			delay = 50
		}
		checkpoint["outputs"], checkpoint["max_delay_ms"] = "at_checkpoint", delay
	}
	return map[string]any{"checkpoint": checkpoint}
}

// fixtureRelay is a relay as data: its settings and its channels body.
type fixtureRelay struct {
	spec     flezha.RelaySpec
	channels func(c *flezha.ChannelBuilder)
}

func (r fixtureRelay) Spec() flezha.RelaySpec            { return r.spec }
func (r fixtureRelay) Channels(c *flezha.ChannelBuilder) { r.channels(c) }

// textIn is an entry channel of UTF-8 text.
func textIn(c *flezha.ChannelBuilder, name string) flezha.Stream[string] {
	return flezha.Input[string](c, name, flezha.InputOptions{Codec: flezha.Text})
}

// stepsReleased answers whether the steps e2e run released its held stages.
// The variable is set at boot, so the gate is a boot-time decision.
func stepsReleased() bool {
	_, ok := os.LookupEnv("FLEZHA_STEPS_E2E_RELEASE")
	return ok
}
