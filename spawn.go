package flezha

import (
	"encoding/json"

	"github.com/flezha/flezha_go_sdk/internal/native"
)

// SpawnHandle is the answer to one SpawnSteps request. Read it inside the
// invocation that made it: once that invocation completes the core releases
// the handle, a failure nobody read is logged once instead, and a later read
// fails. Reading is optional.
type SpawnHandle struct {
	bridge coreBridge
	handle uint64
}

// SpawnOutcome is what a spawn request did.
type SpawnOutcome string

const (
	Spawned         SpawnOutcome = "spawned"
	Delivered       SpawnOutcome = "delivered"
	SkippedOnReplay SpawnOutcome = "skipped_on_replay"
)

// SpawnResult is the instance a spawn request reached, and how.
type SpawnResult struct {
	ID      string       `json:"id"`
	Outcome SpawnOutcome `json:"outcome"`
}

// Wait waits for the controller's answer, up to the core's own bound. A spawn
// that reached no instance fails with the controller's code (such as
// relay.already_running) or the core's (spawn.timeout, spawn.unavailable).
func (h *SpawnHandle) Wait() (SpawnResult, error) {
	result, err := h.read(native.WaitCoreBound)
	if err != nil || result == nil {
		return SpawnResult{}, err
	}
	return *result, nil
}

// TryResult is the answer if it has arrived, or nil while it is pending.
// It never waits.
func (h *SpawnHandle) TryResult() (*SpawnResult, error) { return h.read(0) }

func (h *SpawnHandle) read(timeoutMs uint64) (*SpawnResult, error) {
	answer, err := h.bridge.SpawnResult(h.handle, timeoutMs)
	if err != nil || answer == nil {
		return nil, err
	}
	var result SpawnResult
	if err := json.Unmarshal(answer, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
