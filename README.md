# Flezha Go SDK

The Go SDK for [Flezha](https://github.com/flezha/flezha): relays written as Go types, served by a
runner that links `flz_core`, the runner kernel the Rust, Java, Node, Python, Ruby, PHP and Elixir
SDKs also drive, into one executable.

**Support tier:** [tier 2](https://github.com/flezha/flezha/blob/main/docs/public/publishing/maturity.md#sdk-go):
it passes the full conformance run exactly as tier 1 does, with every capability present, and is
permanently supported; it is not yet run by the maintainer in a live deployment.

## Install

```sh
go get github.com/flezha/flezha_go_sdk
```

The module is not published yet. Until it is, build from a Flezha checkout, as
[`examples/echo`](examples/echo/README.md#building-from-a-checkout) shows.

## Supported Go versions

Go **1.26 or newer**, the release `go.mod` names. The SDK needs nothing beyond the standard library.

## The native library

The core is linked statically through cgo from `libflz_core_ffi.a`, so a built runner is one
executable that loads no core library at run time. That makes cgo, and therefore a C toolchain, a
requirement: `CGO_ENABLED=0` builds are not supported. The directory holding the library comes from
`CGO_LDFLAGS`. In a Flezha checkout, `scripts/build-go-sdk.sh` builds it and prints that line:

```sh
export CGO_LDFLAGS="-L<flezha>/_build/.target/debug/go"
go test ./...
```

Prebuilt libraries for `go get` users come with the first published release.

## Writing a relay

```go
type Echo struct{}

func (Echo) Spec() flezha.Spec { return flezha.Spec{Name: "echo"} }

func (Echo) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit("echo: " + in)
}

type EchoRelay struct{}

func (EchoRelay) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "echo"} }

func (EchoRelay) Channels(c *flezha.ChannelBuilder) {
	in := flezha.Input[string](c, "in", flezha.InputOptions{Codec: flezha.Text})
	flezha.Into(in, Echo{}).Output("out")
}
```

```go
runner, err := flezha.CreateRunner("default")
if err != nil {
	return err
}
if err := runner.RegisterFactory(EchoRelay{}); err != nil {
	return err
}
return runner.Run()
```

A value leaves a tasklet only through `ctx.Emit`. A stream wired into a tasklet that cannot take
its values fails `go build`. The guide is `docs/internal/sdk/go_sdk.md` in the Flezha repository.

## Local mode, testing and the relay API

- **Local mode**: `flezha.StartLocal` runs a relay's declared graph in your own process, with no
  controller. See
  [Local mode](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#local-mode).
- **Testing**: `flezhatest.New`, from `github.com/flezha/flezha_go_sdk/flezhatest`, drives one
  tasklet and checks its replay. See
  [Check a tasklet on its own](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#check-a-tasklet-on-its-own).
- **The relay API**: a relay's entry channels, answering a caller, `flezha.Call` to another relay,
  and `runner.Send` and `runner.Call` from application code. See
  [Sending and calling](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/sending_and_calling.md).

## Licence

Apache-2.0. See the repository for the full text and third-party notices.
