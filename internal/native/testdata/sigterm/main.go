// A program that starts the core and sends itself SIGTERM, run by
// signal_test.go. With the argument `host`, Go owns the signals and hands the
// first one to the core; without it the core installs its own handler.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flezha/flezha_go_sdk/internal/native"
)

func main() {
	hostOwns := len(os.Args) > 1 && os.Args[1] == "host"
	if hostOwns {
		if err := native.HostOwnsSignals(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		go func() { native.Terminate((<-signals).String()) }()
	}
	ctx, err := native.NewContext()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	// No controller listens, so the start keeps dialling; the core has decided
	// on its signal handler before the first dial.
	go func() { _ = ctx.Start("signals") }()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("sending SIGTERM")
	_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	time.Sleep(10 * time.Second)
	fmt.Println("still running after SIGTERM")
	os.Exit(4)
}
