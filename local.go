package flezha

import (
	"log/slog"
	"time"

	"github.com/flezha/flezha_go_sdk/internal/codec"
	"github.com/flezha/flezha_go_sdk/internal/native"
)

// LocalRelay runs a declared relay graph in this process, with no controller:
// the deployed machinery (entry decode, edge transforms and keying, one cell
// per key with its own mailbox, the retry ladder, inter-tasklet delivery) in
// memory, with nothing durable. Durability, plugins, placement, the dead-letter
// queue and scale need a controller; the run says so through the logger every
// time it starts.
type LocalRelay struct {
	core       *native.Context
	dispatcher *dispatcher
	log        *slog.Logger
	sinks      map[string]func(LocalOutput)
	closed     bool
}

// LocalOutput is one value a graph released on a declared output.
type LocalOutput struct {
	Channel string
	Payload []byte
	// What a deployed run would have put on the wire, `flz.*` stamps included.
	Metadata map[string]string
}

// StartLocal registers relay's graph and starts it locally.
func StartLocal(relay Relay, opts ...RunnerOption) (*LocalRelay, error) {
	logger := loggerOf(opts)
	core, err := native.NewContext()
	if err != nil {
		return nil, err
	}
	drainNotices(logger)
	reg := newRegistry()
	if err := startLocal(core, reg, relay); err != nil {
		drainNotices(logger)
		core.Free()
		return nil, err
	}
	// The local-mode banner, said as the run starts.
	drainNotices(logger)
	return &LocalRelay{
		core: core, log: logger, sinks: map[string]func(LocalOutput){},
		dispatcher: &dispatcher{registry: reg, core: core, log: logger},
	}, nil
}

func startLocal(core *native.Context, reg *registry, relay Relay) error {
	spec, err := declareRelay(core, reg, relay)
	if err != nil {
		return err
	}
	// Not Start, which is the one call that dials a controller.
	return core.LocalStart(spec.relay.FactoryID, spec.relay.FactoryID+"-local")
}

// OnOutput delivers every value released on the output channel to callback,
// in release order. A channel the declaration does not carry is refused.
func (l *LocalRelay) OnOutput(channel string, callback func(LocalOutput)) error {
	if err := l.core.LocalOnSink(channel); err != nil {
		return err
	}
	l.sinks[channel] = callback
	return nil
}

// Feed feeds one value's wire bytes to a declared entry channel.
func (l *LocalRelay) Feed(channel string, payload []byte) error {
	return l.core.LocalFeed(channel, payload)
}

// localPoll is one poll turn: short, because a turn that hands nothing back is
// a probe of whether the graph settled.
const localPoll = 20 * time.Millisecond

// RunUntilIdle drives the graph until it has nothing left to do: take an
// activation if there is one and answer it, hand on what the outputs released,
// and when a turn hands nothing back ask the core whether the graph settled.
// A budget that elapses first is reported, never passed off as done.
func (l *LocalRelay) RunUntilIdle(budget time.Duration) error {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		status, activation, err := l.core.Poll(uint64(localPoll.Milliseconds()))
		if err != nil {
			return err
		}
		if status == native.PollActivation {
			l.dispatcher.dispatch(activation)
			if err := l.drainOutputs(); err != nil {
				return err
			}
			continue
		}
		if err := l.drainOutputs(); err != nil {
			return err
		}
		if l.core.LocalRunUntilIdle(uint64(localPoll.Milliseconds())) == nil {
			// One last pass: a value released on the turn that settled is still queued.
			return l.drainOutputs()
		}
	}
	return refusal("runtime.local_run_unanswered", map[string]any{"budget_ms": budget.Milliseconds()})
}

// Close ends the run and releases its cells. Idempotent.
func (l *LocalRelay) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	// Stop first, then take what the run released and the loop had not reached.
	err := l.core.LocalStop()
	if drained := l.drainOutputs(); err == nil {
		err = drained
	}
	drainNotices(l.log)
	// Not Shutdown: that stops the process-wide poll core, and a process may
	// start another local run after this one.
	l.core.Free()
	return err
}

// drainOutputs hands on everything the outputs have released.
func (l *LocalRelay) drainOutputs() error {
	for {
		status, released, err := l.core.LocalNextOutput(0)
		if err != nil || status != native.PollActivation {
			return err
		}
		output, err := codec.DecodeOutput(released)
		if err != nil {
			return err
		}
		callback := l.sinks[output.PluginChannel]
		if !output.Plugin || callback == nil {
			continue
		}
		metadata := make(map[string]string, len(output.Metadata))
		for _, pair := range output.Metadata {
			metadata[pair[0]] = pair[1]
		}
		callback(LocalOutput{Channel: output.PluginChannel, Payload: output.Payload, Metadata: metadata})
	}
}
