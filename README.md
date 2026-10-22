# Flezha Go SDK

The Go SDK for [Flezha](https://github.com/flezha/flezha): relays written as Go types, served by a
runner that links Flezha's shared core into one executable.

**Support tier:** [tier 2](https://github.com/flezha/flezha/blob/main/docs/public/publishing/maturity.md#sdk-go):
it passes the full conformance run exactly as tier 1 does, with every capability present, and is
permanently supported; it is not yet run by the maintainer in a live deployment.

```go
type Echo struct{}

func (Echo) Spec() flezha.Spec { return flezha.Spec{Name: "echo"} }

func (Echo) Run(in string, ctx *flezha.Ctx[flezha.NoState, string]) error {
	return ctx.Emit(in)
}

type EchoRelay struct{}

func (EchoRelay) Spec() flezha.RelaySpec { return flezha.RelaySpec{FactoryID: "echo"} }

func (EchoRelay) Channels(c *flezha.ChannelBuilder) {
	in := flezha.Input[string](c, "input", flezha.InputOptions{Codec: flezha.Text})
	flezha.Into(in, Echo{}).Output("out")
}
```

## Building

The SDK needs only the Go standard library, the Go release its `go.mod` names or newer, and a C
toolchain: the core is linked statically through cgo from `libflz_core_ffi.a`, whose directory
`CGO_LDFLAGS` names. In a Flezha checkout, `scripts/build-go-sdk.sh` builds the library and the test
app and prints that line. Prebuilt libraries for `go get` users come with the first published
release.

```sh
export CGO_LDFLAGS="-L<flezha>/_build/.target/debug/go"
go test ./...
```

The guide is `docs/internal/sdk/go_sdk.md` in the Flezha repository.
