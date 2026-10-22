package flezha_test

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	flezha "github.com/flezha/flezha_go_sdk"
	"github.com/flezha/flezha_go_sdk/flezhatest"
)

// The code docs/internal/sdk/go_sdk.md shows, compiled and run here so the
// guide cannot drift from the SDK.

type Order struct {
	Pair string `json:"pair"`
	Qty  int    `json:"qty"`
}

type Book struct {
	Sum int `json:"sum"`
}

type Added struct {
	Qty int `json:"qty"`
}

func (a Added) ApplyTo(b *Book) { b.Sum += a.Qty }

type Totals struct{}

func (Totals) Spec() flezha.Spec { return flezha.Spec{Name: "totals", IdleTTLSeconds: 3600} }

func (Totals) Changes() flezha.Changes[Book] { return flezha.Changes[Book]{"added": Added{}} }

func (Totals) Run(in Order, ctx *flezha.Ctx[Book, int]) error {
	if err := ctx.Change(Added{Qty: in.Qty}); err != nil {
		return err
	}
	return ctx.Emit(ctx.State().Sum)
}

type Orders struct{}

func (Orders) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "orders"} }

func (Orders) Channels(c *flezha.ChannelBuilder) {
	orders := flezha.Input[Order](c, "orders").KeyBy(func(o Order) string { return o.Pair })
	flezha.Into(orders, Totals{}).Output("totals")
}

func ExampleStartLocal() {
	run, err := flezha.StartLocal(Orders{}, flezha.WithLogger(quietLogger()))
	if err != nil {
		panic(err)
	}
	defer run.Close()
	_ = run.OnOutput("totals", func(out flezha.LocalOutput) { fmt.Println(string(out.Payload)) })
	_ = run.Feed("orders", []byte(`{"pair":"EUR/USD","qty":2}`))
	_ = run.Feed("orders", []byte(`{"pair":"EUR/USD","qty":3}`))
	if err := run.RunUntilIdle(10 * time.Second); err != nil {
		panic(err)
	}
	// Output:
	// 2
	// 5
}

func ExampleShortRelay() {
	upcase := flezha.ShortRelay("upcase", func(in string, _ *flezha.Ctx[flezha.NoState, string]) (string, bool) {
		return strings.ToUpper(in), in != ""
	}, flezha.ShortOptions{Codec: flezha.Text, Output: "out"})
	run, err := flezha.StartLocal(upcase, flezha.WithLogger(quietLogger()))
	if err != nil {
		panic(err)
	}
	defer run.Close()
	_ = run.OnOutput("out", func(out flezha.LocalOutput) { fmt.Println(string(out.Payload)) })
	for _, in := range []string{"", "eur/usd"} {
		_ = run.Feed("input", []byte(in))
	}
	if err := run.RunUntilIdle(10 * time.Second); err != nil {
		panic(err)
	}
	// Output:
	// EUR/USD
}

func Example_harness() {
	h, err := flezhatest.New[Order, Book, int](Totals{})
	if err != nil {
		panic(err)
	}
	out, _ := h.Feed(Order{Pair: "EUR/USD", Qty: 4}, 0, false)
	fmt.Println(out.Outputs, h.State().Sum, h.ReplayEquals())
	// Output: [4] 4 <nil>
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
