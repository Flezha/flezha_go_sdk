// Package harness is the seam between the SDK and its test harness: the SDK
// fills New at init, and package flezhatest drives one tasklet through it
// with the SDK's own dispatch path, never a second implementation of it.
package harness

// Engine drives one tasklet's cell without a controller.
type Engine interface {
	// Feed runs the tasklet on one input; replay replays the next live input.
	Feed(input any, now uint64, replay bool) (Outcome, error)
	// FireTimer runs OnTimer as the core would fire it.
	FireTimer(eventTime bool, scheduledAt uint64) (Outcome, error)
	// Compensate runs the declared compensation over recorded outputs.
	Compensate(outputs [][]byte) (Outcome, error)
	// State is the cell's live state.
	State() any
	// Restore rebuilds the state as a restore does, from a snapshot and a change log.
	Restore(snapshot []byte, changes [][]byte) error
	// Replayed folds the recorded change log into a fresh initial state.
	Replayed() (any, error)
	// DecodeChange is a change record as the tasklet's change value.
	DecodeChange(body []byte) (any, error)
	// Cell names the cell the engine drives.
	Cell() string
}

// Outcome is what one invocation produced.
type Outcome struct {
	Outputs [][]byte
	Changes [][]byte
	Timers  []Timer
}

// Timer is one timer operation an invocation recorded.
type Timer struct {
	EventTime   bool
	ScheduledAt uint64
	Register    bool
}

// New starts an engine over the one-tasklet relay the harness built: the cell
// under cellKey (nil for a single cell) of relay instance relayID.
var New func(relay any, cellKey *string, relayID string) (Engine, error)
