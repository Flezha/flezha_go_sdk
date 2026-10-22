# Tradedesk: the Go project of the tradedesk tutorial

The toy trader the [tradedesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/tradedesk.md) builds, as
a standalone project. `go.mod` names one released version of
`github.com/flezha/flezha_go_sdk`, the way your own project would.

It holds the desk's two relays and the runner that serves them. The tutorial
writes them one tasklet at a time.

| File | What is in it |
|---|---|
| `main.go` | the runner: this process joins the `trading` namespace and registers both factories |
| `trade_watcher.go` | the ETL stream, a flow relay: the exchange's trade messages in, a cell per symbol, buy signals out |
| `track_prices.go` | keeps each symbol's last ten prices, and compares a new price with their average |
| `buy_above_average.go` | starts an order when a price is 2 % or more above its average |
| `order_executor.go` | the workflow, a steps relay: two steps in order, then two side by side. The order an instance is started with is the `OrderTicket` struct here |
| `reserve_funds.go` | step one: puts the money aside, and releases it again if the order is undone |
| `place_order.go` | step two: places the paper order, and cancels it if the order is undone |
| `confirm_fill.go` | waits ten seconds for the exchange, confirms the fill and ends the order with its result, and refuses an order above the exchange's limit |
| `record_order.go` | books the placed order, and has nothing to undo |

Every tasklet is your own type, with a `Spec` naming it and a `Run`. What a
tasklet remembers, the changes that record it, and the values it takes and hands
on sit in its own file, beside it.

## Run it

You need a controller running, the `trading` namespace created and a key for it
([Open the desk](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/open_the_desk.md) does both).

```sh
FLEZHA_NAMESPACE_SECRET=<trading key> go run .
```

It blocks until shutdown, which is a SIGTERM or Ctrl-C. The namespace is the
first argument and defaults to `trading`: `go run . my_desk`. The runner logs
through `log/slog` at `FLEZHA_LOG_LEVEL`, `warn` unless you set it.

From another terminal, the controller lists it:

```sh
flz runner list --namespace trading
```

## Start an order by hand

One instance of `order_executor` is one order. `--text` carries the order on the
`order` channel:

```sh
flz relay start order_executor --id order_1 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
```

The runner's terminal shows the order being booked. The ids come from
`ctx.StableID()`, so yours differ:

```
[book] EUR/USD 10 as ord-6b2e13f521b6781afd9f710152cd2f25
```

About ten seconds later, once the exchange has answered, the instance ends on
its own, and `flz relay info`, which lists every relay of the namespace, shows
how:

```sh
flz relay info --namespace trading
```

```
...
Relay: order_1
  Run: steps
  Phase: finished
  Status: ok
  Finished At (unix s): 1792694526
```

## Watch it undo itself

The paper exchange fills at most 100 at a time, so `confirm_fill` refuses a
larger order. Start one the same way, with a quantity above the limit:

```sh
flz relay start order_executor --id order_2 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 500}' --channel order
```

`confirm_fill` is tried three times, ten seconds each, and then the order ends
in failure. Every step that had finished is undone, in reverse: the order is
cancelled, then the money is released. `record_order` has no undo, so the
booking stays:

```
[book] EUR/USD 500 as ord-0e7ee9553cfe5eac606fd232fd7ab33c
time=2026-10-22T20:44:51.327+02:00 level=WARN msg="tasklet confirm_fill attempt 1/3 failed (retrying in 42 ms): the exchange fills at most 100 a time, and this is 500" target=flz_core::retry
time=2026-10-22T20:45:01.330+02:00 level=WARN msg="tasklet confirm_fill attempt 2/3 failed (retrying in 74 ms): the exchange fills at most 100 a time, and this is 500" target=flz_core::retry
[undo] cancelled ord-0e7ee9553cfe5eac606fd232fd7ab33c
[undo] released res-54e4641a002cc27f8d030507d8d5aea5
```

```sh
flz relay info --namespace trading
```

```
...
Relay: order_2
  Run: steps
  Phase: finished
  Status: error
  Error: the exchange fills at most 100 a time, and this is 500
  Finished At (unix s): 1792694711
```

## Building from a checkout

`github.com/flezha/flezha_go_sdk` is not published yet, and neither is the core
library it links through cgo, so a C toolchain and a Flezha checkout are needed.
From the root of the checkout, build the core once:

```sh
scripts/build-go-sdk.sh --release
```

Then, from this folder, point Go at the checkout, with its absolute path:

```sh
go work init .
go work edit -replace github.com/flezha/flezha_go_sdk=/path/to/flezha/sdk/go
export CGO_LDFLAGS="-L/path/to/flezha/_build/.target/release/go"
```

Git ignores the `go.work` file, and once the module is published the step goes
away and `go run .` is all you need.

## Where to go next

The echo in [`../echo`](../echo/README.md) is the project of the getting-started pages, and
[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what a tasklet, a relay and a runner each are.
