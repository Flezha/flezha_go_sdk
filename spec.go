package flezha

// Spec is everything a tasklet declares that is not behaviour, read once when
// the relay wiring it registers, never per message. Name is required: it is
// part of every cell's durable path, so renaming the Go type never moves
// saved state. A zero field leaves the core's default.
//
// The cell mode is not declared here: a tasklet fed through KeyBy is dynamic,
// one with CellKeys static, and any other single.
type Spec struct {
	Name string
	// A static key set; the tasklet implements CellsFor to route an input.
	CellKeys []string
	// Evict an idle dynamic cell after this long; 0 never.
	IdleTTLSeconds int64
	MaxCells       int
	// Serve the cell's change sequence as Now().
	LogicalClock bool
	// The per-cell mailbox capacity; nil is the core's default. A pointer, so
	// a capacity of 0 can be written and refused by name.
	MailboxCapacity *int
	// The schema version the state and change records are written at; 0 is 1.
	SchemaVersion uint32
	// The oldest version still read; 0 is SchemaVersion.
	MinReadableSchemaVersion uint32
	// The pending timers one cell may hold; 0 is the core's default.
	MaxTimersPerCell int
	// The cell log size that asks for a snapshot; 0 is the controller's default.
	CompactAfterBytes int64
}

// Tasklet is one processing step: a type with a Spec and a Run. A value leaves
// it only through ctx.Emit. Run returning nil completes the item; any other
// error is retried up the channel's retry ladder, and one wrapped by
// NonRetryable parks at once. A panic is recovered and retried as an error
// carrying its text.
//
// A hook is declared by implementing it: InitState, Changes, OnTimer,
// EventTime, CellsFor, Compensate, MigrateState and MigrateChange, each with
// the tasklet's own I, S and O.
type Tasklet[I, S, O any] interface {
	Spec() Spec
	Run(in I, ctx *Ctx[S, O]) error
}

// NoState is the state of a stateless tasklet.
type NoState struct{}

// NoOutput is the output of a tasklet that emits nothing.
type NoOutput struct{}

// Change is one recorded change of a cell's state: ApplyTo folds it in, live
// and on restore alike, so it must read nothing but the state and itself.
type Change[S any] interface {
	ApplyTo(state *S)
}

// Changes lists a tasklet's change types under explicit names. A record
// carries the name, never the Go type's, so a type rename keeps every saved
// record readable. Each value is a zero value of its type.
type Changes[S any] map[string]Change[S]

// TimerFiring is a fired timer, as OnTimer receives it. ctx.Now() inside the
// hook equals ScheduledAt, so a chained TimerAfter is replay-stable.
type TimerFiring struct {
	Kind        TimerKind
	ScheduledAt uint64
}

// TimerKind is the clock a timer was scheduled against.
type TimerKind string

const (
	EventTime      TimerKind = "event_time"
	ProcessingTime TimerKind = "processing_time"
)

// The hooks, found on a tasklet by type assertion when it is wired.
type (
	initStateHook[S any] interface {
		InitState(cellKey *string) S
	}
	changesHook[S any] interface {
		Changes() Changes[S]
	}
	onTimerHook[S, O any] interface {
		OnTimer(timer TimerFiring, ctx *Ctx[S, O]) error
	}
	eventTimeHook[I any] interface {
		EventTime(in I) uint64
	}
	cellsForHook[I any] interface {
		CellsFor(in I) []string
	}
	compensateHook[S, O any] interface {
		Compensate(outputs []O, ctx *Ctx[S, O]) error
	}
	migrateStateHook[S any] interface {
		MigrateState(fromVersion uint32, bytes []byte) (S, error)
	}
	migrateChangeHook[S any] interface {
		MigrateChange(fromVersion uint32, name string, value []byte) (Change[S], error)
	}
)

// RelaySpec is the factory id a relay registers under and its class settings,
// each written as the settings tree's own data: Run "steps" or
// map[string]any{"steps": map[string]any{"each": "per_input"}}, Durable false,
// "local" or map[string]any{"local": map[string]any{"sleep_after": 2000}}.
// Unwritten, the class runs as a flow and keeps its work locally. The core
// reads the tree at registration and refuses a misplaced setting by name.
type RelaySpec struct {
	FactoryID     string
	Run           any
	Durable       any
	MaxDurationMs int64
	Description   string
}

// Relay is a relay type: its RelaySpec and its channels graph.
type Relay interface {
	Spec() RelaySpec
	Channels(c *ChannelBuilder)
}
