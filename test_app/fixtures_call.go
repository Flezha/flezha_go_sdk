// Twins of the reference's relay call fixtures
// (test/apps/rust_runner/src/relay_call.rs), under the same factory ids,
// tasklet names, channel names and payloads: relays that call another relay
// from their graph and read the outcome as a flezha.Response.
//
//   - call_pricing: a flow callee answering `price:<input>` on its output
//     `quotes`; an input starting `silent` is never answered.
//   - call_orders: a flow caller calling the instance `pricing`, answered from
//     `quotes`.
//   - call_agent: a durable per-input keep-progress session answering
//     `answer:<turn>`; `boom` fails the turn, `slow` answers after 2 s.
//   - call_gateway: an effectively-once caller calling call_agent by the
//     session key of each `<session>:<text>` turn, and without creating it for
//     a `resume:<session>:<text>` turn, both Response channels wired back into
//     its calling tasklet.
//   - call_tool_session: a durable per-input session calling `pricing` from a
//     turn and reading the outcome as its next turn.
//   - call_orders_steps: call_orders as a durable per-input session, so
//     order_ask's channel ends only in the call.
//
// Every outcome is written to an output as one line,
// `<request>=ok:<answer>@<target>/<created>` or
// `<request>=error:<code>@<target>/<created>`.
package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	flezha "github.com/flezha/flezha_go_sdk"
)

const (
	callDeadlineMs = 30_000
	// The prefix a caller's tasklet puts on an outcome line, which no request
	// carries, so one output channel can feed both the call and the output.
	callOutcome = "response "
	// The prefix of a turn resuming a session that must already run: its call
	// may not create the callee.
	callResume = "resume:"
)

type responseCall = flezha.Response[string, string]

func renderCall(r responseCall) string {
	outcome := "ok:" + r.Value
	if !r.Ok() {
		outcome = "error:" + r.Err.Code
	}
	return fmt.Sprintf("%s=%s@%s/%t", r.Request, outcome, r.Target, r.Created)
}

func isOutcomeCall(line string) bool { return strings.HasPrefix(line, callOutcome) }
func isResumeCall(line string) bool  { return strings.HasPrefix(line, callResume) }
func isTurnCall(line string) bool    { return !isOutcomeCall(line) && !isResumeCall(line) }

// sessionCall is the session a turn or an outcome line belongs to: what
// precedes its `:`.
func sessionCall(line string) string {
	head, _, _ := strings.Cut(strings.TrimPrefix(line, callOutcome), ":")
	return head
}

// callerInCall is what reaches a calling tasklet: a turn from outside, or the
// outcome of a call it made.
type callerInCall struct {
	Turn    *string       `json:"turn,omitempty"`
	Outcome *responseCall `json:"outcome,omitempty"`
}

func turnCall(turn string) callerInCall       { return callerInCall{Turn: &turn} }
func outcomeCall(r responseCall) callerInCall { return callerInCall{Outcome: &r} }

// pass is what a caller's tasklet emits: a turn as it came, an outcome as its line.
func (in callerInCall) pass() string {
	if in.Outcome != nil {
		return callOutcome + renderCall(*in.Outcome)
	}
	if in.Turn != nil {
		return *in.Turn
	}
	return ""
}

// -- call_pricing ------------------------------------------------------------

type pricingQuoteCall struct{}

func (pricingQuoteCall) Spec() flezha.Spec { return flezha.Spec{Name: "pricing_quote"} }

func (pricingQuoteCall) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	if strings.HasPrefix(in, "silent") {
		return nil
	}
	return ctx.Emit("price:" + in)
}

type callPricingCall struct{}

func (callPricingCall) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "call_pricing"} }

func (callPricingCall) Channels(c *flezha.ChannelBuilder) {
	in := flezha.Input[string](c, "in", flezha.InputOptions{Codec: flezha.Text})
	flezha.Into(in, pricingQuoteCall{}).Output("quotes")
}

// -- call_orders -------------------------------------------------------------

type orderAskCall struct{}

func (orderAskCall) Spec() flezha.Spec { return flezha.Spec{Name: "order_ask"} }

func (orderAskCall) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in)
}

type orderPricedCall struct{}

func (orderPricedCall) Spec() flezha.Spec { return flezha.Spec{Name: "order_priced"} }

func (orderPricedCall) Run(in responseCall, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(renderCall(in))
}

type callOrdersCall struct{}

func (callOrdersCall) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "call_orders"} }

func (callOrdersCall) Channels(c *flezha.ChannelBuilder) {
	in := flezha.Input[string](c, "in", flezha.InputOptions{Codec: flezha.Text})
	order := flezha.Into(in, orderAskCall{}).Named("order")
	priced := flezha.Call[string, string](order, flezha.CallSpec{
		Target: "pricing", DeadlineMs: callDeadlineMs, AnswerChannel: "quotes", Codec: flezha.Text,
	})
	flezha.Into(priced, orderPricedCall{}).Output("priced")
}

// sessionSettingsCall is the durable per-input session the steps callers run as.
func sessionSettingsCall(factoryID string) flezha.RelaySpec {
	return flezha.RelaySpec{
		FactoryID:     factoryID,
		Run:           map[string]any{"steps": map[string]any{"each": "per_input"}},
		Durable:       fixtureDurable(),
		MaxDurationMs: 600_000,
	}
}

type callOrdersStepsCall struct{}

func (callOrdersStepsCall) Spec() flezha.RelaySpec { return sessionSettingsCall("call_orders_steps") }

func (callOrdersStepsCall) Channels(c *flezha.ChannelBuilder) { callOrdersCall{}.Channels(c) }

// -- call_agent --------------------------------------------------------------

type agentTurnCall struct{}

func (agentTurnCall) Spec() flezha.Spec { return flezha.Spec{Name: "agent_turn"} }

func (agentTurnCall) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	text := in
	if _, after, found := strings.Cut(in, ":"); found {
		text = after
	}
	switch text {
	case "boom":
		return flezha.NonRetryable(errors.New("the turn failed"))
	case "slow":
		time.Sleep(2 * time.Second)
	}
	return ctx.Emit("answer:" + in)
}

type callAgentCall struct{}

func (callAgentCall) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{
		FactoryID:     "call_agent",
		Run:           map[string]any{"steps": map[string]any{"each": "per_input", "on_failure": "keep_progress"}},
		Durable:       fixtureDurable(),
		MaxDurationMs: 600_000,
	}
}

func (callAgentCall) Channels(c *flezha.ChannelBuilder) {
	turn := flezha.Input[string](c, "turn", flezha.InputOptions{Codec: flezha.Text})
	flezha.Into(turn, agentTurnCall{}).Output("")
}

// -- call_gateway ------------------------------------------------------------

type gatewayAccountCall struct{}

func (gatewayAccountCall) Spec() flezha.Spec { return flezha.Spec{Name: "gateway_account"} }

func (gatewayAccountCall) Run(in callerInCall, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in.pass())
}

type gatewayResumeCall struct{}

func (gatewayResumeCall) Spec() flezha.Spec { return flezha.Spec{Name: "gateway_resume"} }

func (gatewayResumeCall) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(strings.TrimPrefix(in, callResume))
}

type gatewayLogCall struct{}

func (gatewayLogCall) Spec() flezha.Spec { return flezha.Spec{Name: "gateway_log"} }

func (gatewayLogCall) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(strings.TrimPrefix(in, callOutcome))
}

type callGatewayCall struct{}

func (callGatewayCall) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{
		FactoryID: "call_gateway",
		Run:       map[string]any{"flow": map[string]any{"exactly": "effectively_once"}},
	}
}

func (callGatewayCall) Channels(c *flezha.ChannelBuilder) {
	in := flezha.Input[string](c, "in", flezha.InputOptions{Codec: flezha.Text})
	account := flezha.Into(flezha.Map(in, turnCall), gatewayAccountCall{}).Named("gateway")

	answered := flezha.Call[string, string](account.Filter(isTurnCall).KeyBy(sessionCall), flezha.CallSpec{
		Factory: "call_agent", DeadlineMs: callDeadlineMs, Codec: flezha.Text,
	})
	flezha.Into(flezha.Map(answered, outcomeCall), gatewayAccountCall{})

	resume := flezha.Into(account.Filter(isResumeCall), gatewayResumeCall{}).Named("resume")
	resumed := flezha.Call[string, string](resume.KeyBy(sessionCall), flezha.CallSpec{
		Factory: "call_agent", DeadlineMs: callDeadlineMs, NoCreate: true, Codec: flezha.Text,
	})
	flezha.Into(flezha.Map(resumed, outcomeCall), gatewayAccountCall{})

	flezha.Into(account.Filter(isOutcomeCall).KeyBy(sessionCall), gatewayLogCall{}).
		Keyed().
		Output("gateway_out")
}

// -- call_tool_session -------------------------------------------------------

type toolTurnCall struct{}

func (toolTurnCall) Spec() flezha.Spec { return flezha.Spec{Name: "tool_turn"} }

func (toolTurnCall) Run(in callerInCall, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in.pass())
}

type toolLogCall struct{}

func (toolLogCall) Spec() flezha.Spec { return flezha.Spec{Name: "tool_log"} }

func (toolLogCall) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(strings.TrimPrefix(in, callOutcome))
}

type callToolSessionCall struct{}

func (callToolSessionCall) Spec() flezha.RelaySpec { return sessionSettingsCall("call_tool_session") }

func (callToolSessionCall) Channels(c *flezha.ChannelBuilder) {
	in := flezha.Input[string](c, "turn", flezha.InputOptions{Codec: flezha.Text})
	turn := flezha.Into(flezha.Map(in, turnCall), toolTurnCall{}).Named("tool")
	request := turn.Filter(func(line string) bool { return !isOutcomeCall(line) })
	answered := flezha.Call[string, string](request, flezha.CallSpec{
		Target: "pricing", DeadlineMs: callDeadlineMs, AnswerChannel: "quotes", Codec: flezha.Text,
	})
	flezha.Into(flezha.Map(answered, outcomeCall), toolTurnCall{})
	flezha.Into(turn.Filter(isOutcomeCall), toolLogCall{}).Output("tool_out")
}

// -- orders (relay_api) ------------------------------------------------------

type orderToPlaceCall struct {
	Pair   string  `json:"pair"`
	Amount float64 `json:"amount"`
}

type orderPlacedCall struct {
	Pair   string  `json:"pair"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

type ordersPlaceCall struct{}

func (ordersPlaceCall) Spec() flezha.Spec { return flezha.Spec{Name: "orders_place"} }

func (ordersPlaceCall) Run(in orderToPlaceCall, ctx *flezha.Ctx[flezha.NoState, orderPlacedCall]) error {
	return ctx.Emit(orderPlacedCall{Pair: in.Pair, Amount: in.Amount, Status: "placed"})
}

type ordersAuditCall struct{}

func (ordersAuditCall) Spec() flezha.Spec { return flezha.Spec{Name: "orders_audit"} }

func (ordersAuditCall) Run(string, *flezha.Ctx[flezha.NoState, flezha.NoOutput]) error { return nil }

// ordersCall is the relay_api fixture: `place` reads a JSON order, is
// described, and answers through `placed`; `audit` reads text and answers
// nothing.
type ordersCall struct{}

func (ordersCall) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "orders"} }

func (ordersCall) Channels(c *flezha.ChannelBuilder) {
	place := flezha.Input[orderToPlaceCall](c, "place", flezha.InputOptions{Description: "Places an order"})
	flezha.Into(place, ordersPlaceCall{}).Output("placed")
	audit := flezha.Input[string](c, "audit", flezha.InputOptions{Codec: flezha.Text})
	flezha.Into(audit, ordersAuditCall{})
}

func callFixtures() []flezha.Relay {
	return []flezha.Relay{
		callPricingCall{}, callOrdersCall{}, callAgentCall{},
		callGatewayCall{}, callToolSessionCall{}, callOrdersStepsCall{},
	}
}

func callAPIFixtures() []flezha.Relay { return []flezha.Relay{ordersCall{}} }
