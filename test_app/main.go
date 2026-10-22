// The Go SDK's e2e test app: registers the reference fixture twins and runs
// until shutdown. The namespace is os.Args[1] (default "default"), as for every
// other runner of the conformance suite.
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	flezha "github.com/flezha/flezha_go_sdk"
)

func main() {
	// The conformance run answers a file and exits: no namespace, no controller.
	if report := os.Getenv("FLEZHA_CONFORMANCE_REPORT"); report != "" {
		if err := writeReport(report); err != nil {
			fmt.Fprintln(os.Stderr, "[go_runner] conformance report failed:", err)
			os.Exit(1)
		}
		return
	}
	namespace := "default"
	if len(os.Args) > 1 {
		namespace = os.Args[1]
	}
	// The app narrates at info unless the run says otherwise.
	if os.Getenv("FLEZHA_LOG_LEVEL") == "" {
		_ = os.Setenv("FLEZHA_LOG_LEVEL", "info")
	}
	runner, err := flezha.CreateRunner(namespace)
	if err != nil {
		fail(err)
	}
	if script := os.Getenv("FLEZHA_LOCAL_SEND_SCRIPT"); script != "" {
		localSendDrill(runner, script)
	}
	if row := os.Getenv("FLEZHA_REFUSAL_FIXTURE"); row != "" {
		// One row of the refusal table instead of the fixture set: a refused
		// topology dies at declaration, before the core is started.
		err = registerOnly(runner, row)
	} else {
		err = registerAll(runner)
	}
	if err != nil {
		fail(err)
	}
	transport := os.Getenv("FLEZHA_TRANSPORT")
	if transport == "" {
		transport = "uds"
	}
	fmt.Fprintf(os.Stderr, "[go_runner] namespace=%s transport=%s\n", namespace, transport)
	if err := runner.Run(); err != nil {
		fail(err)
	}
}

// allRelays is every fixture this app registers.
func allRelays() []flezha.Relay {
	var relays []flezha.Relay
	for _, group := range [][]flezha.Relay{
		coreFixtures(), stateFixtures(), probeFixtures(), stepsFixtures(),
		stepsLoopFixtures(), partFixtures(), callFixtures(), callAPIFixtures(),
	} {
		relays = append(relays, group...)
	}
	return relays
}

func registerAll(runner *flezha.Runner) error {
	for _, relay := range allRelays() {
		if err := runner.RegisterFactory(relay); err != nil {
			return fmt.Errorf("registering %s: %w", relay.Spec().FactoryID, err)
		}
	}
	return nil
}

// fail ends the process on a refusal. A refused declaration names itself: its
// codes ride the error as data, and the drill reads them off this output.
func fail(err error) {
	var refused *flezha.Error
	if errors.As(err, &refused) && strings.HasPrefix(refused.Code, "topology.") {
		fmt.Fprintf(os.Stderr, "[go_runner] declaration refused: %s\n%s\n", strings.Join(codesOf(refused), ", "), refused.Message)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "[go_runner]", err)
	os.Exit(1)
}
