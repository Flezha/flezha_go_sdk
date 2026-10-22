// A map whose function takes an int, on a Stream[string].
package main

import flezha "github.com/flezha/flezha_go_sdk"

func channels(c *flezha.ChannelBuilder) {
	flezha.Map(flezha.Input[string](c, "lines"), func(n int) int { return n + 1 })
}

func main() { _ = channels }
