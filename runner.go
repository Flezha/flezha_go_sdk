package flezha

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/flezha/flezha_go_sdk/internal/native"
)

// Runner is what an application starts: the relay types it registers and the
// connection that serves them. One runner per process; N processes are N
// runners, which the controller's placement already spreads relays across.
type Runner struct {
	namespace string
	core      *native.Context
	registry  *registry
	log       *slog.Logger

	signals  sync.Once
	starting atomic.Bool
	started  atomic.Bool
	closing  atomic.Bool
	pool     *pool
	done     chan struct{}
}

// RunnerOption configures CreateRunner and StartLocal.
type RunnerOption func(*runnerOptions)

type runnerOptions struct{ log *slog.Logger }

// WithLogger hands the runner the application's logger: the SDK's diagnostics
// and the core's notices go there, and nothing reaches standard error past it.
func WithLogger(logger *slog.Logger) RunnerOption {
	return func(o *runnerOptions) { o.log = logger }
}

// loggerOf is the logger the options name, or the default one.
func loggerOf(opts []RunnerOption) *slog.Logger {
	var o runnerOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.log == nil {
		return defaultLogger()
	}
	return o.log
}

// CreateRunner creates the runner of namespace. The linked core is checked
// here: a core of another activation schema is refused, naming both versions,
// before anything connects.
func CreateRunner(namespace string, opts ...RunnerOption) (*Runner, error) {
	r := &Runner{namespace: namespace, registry: newRegistry(), done: make(chan struct{}), log: loggerOf(opts)}
	core, err := native.NewContext()
	if err != nil {
		return nil, err
	}
	r.core = core
	// The first take starts holding the core's notices for this runner's drains.
	drainNotices(r.log)
	return r, nil
}

// Namespace is the namespace the runner serves.
func (r *Runner) Namespace() string { return r.namespace }

// RegisterFactory registers a relay type, a full form or a short form alike.
// The declaration is judged by the core here, so a malformed one is refused at
// this call, with the core's code; after Start the controller learns of it at
// once.
func (r *Runner) RegisterFactory(relay Relay) error {
	_, err := declareRelay(r.core, r.registry, relay)
	return err
}

// declareRelay builds relay's graph, has the core judge it, serves it from reg
// and declares it to the core: the one path a relay takes into a runner and
// into a local run.
func declareRelay(core *native.Context, reg *registry, relay Relay) (*classSpec, error) {
	spec, err := specOf(relay)
	if err != nil {
		return nil, err
	}
	if err := spec.check(core); err != nil {
		return nil, err
	}
	if _, err := reg.add(spec); err != nil {
		return nil, err
	}
	if err := core.DeclareClass(spec.declaration()); err != nil {
		reg.remove(spec.relay.FactoryID)
		return nil, err
	}
	return spec, nil
}

// UnregisterFactory undoes a registration. After Start the core refuses a
// relay type with live instances on this runner.
func (r *Runner) UnregisterFactory(relay Relay) error {
	factoryID := relay.Spec().FactoryID
	if r.registry.get(factoryID) == nil {
		return refusal("runtime.class_not_registered", map[string]any{"factory": factoryID})
	}
	if err := r.core.UndeclareClass(factoryID); err != nil {
		return err
	}
	r.registry.remove(factoryID)
	return nil
}

// Start connects and starts serving: everything up to the first poll, and
// nothing that blocks after it. Go owns SIGTERM and SIGINT and hands the first
// to the core, which drains and exits; a closed standard input ends the runner
// the same way.
func (r *Runner) Start() error {
	if !r.starting.CompareAndSwap(false, true) {
		return refusal("runtime.runner_already_running", nil)
	}
	if err := native.HostOwnsSignals(); err != nil {
		r.starting.Store(false)
		return err
	}
	// Taken over before the connect wait, so a signal during it drains too.
	r.signals.Do(func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		go func() { native.Terminate((<-signals).String()) }()
	})
	err := r.core.Start(r.namespace)
	drainNotices(r.log)
	if err != nil {
		// A refused start leaves nothing dialling, so the runner may start again.
		r.starting.Store(false)
		return err
	}
	if stdin, statErr := os.Stdin.Stat(); statErr == nil && stdin.Mode()&os.ModeCharDevice == 0 {
		native.WatchStdinEOF()
	}
	policy, err := r.core.PoolOpen(runtime.NumCPU())
	if err != nil {
		return err
	}
	r.log.Info(fmt.Sprintf("runner up on namespace %s: a pool of %d poller(s), up to %d", r.namespace, policy.Base, policy.Ceiling))
	d := &dispatcher{registry: r.registry, core: r.core, log: r.log}
	r.pool = &pool{core: r.core, policy: policy, serve: d.dispatch, log: r.log, onAnswer: func() { drainNotices(r.log) }}
	r.started.Store(true)
	go func() {
		r.pool.run()
		close(r.done)
	}()
	return nil
}

// AwaitTermination blocks until the runner stops, then releases the core.
func (r *Runner) AwaitTermination() error {
	if !r.started.Load() {
		return refusal("runtime.runner_not_started", nil)
	}
	<-r.done
	drainNotices(r.log)
	if !r.pool.stuck.Load() {
		r.core.Free()
	}
	return nil
}

// Close shuts the runner down: every relay's state ships, what is in flight
// finishes, and AwaitTermination returns.
func (r *Runner) Close() {
	if r.closing.CompareAndSwap(false, true) {
		r.core.Shutdown()
	}
}

// Run is Start followed by AwaitTermination.
func (r *Runner) Run() error {
	if err := r.Start(); err != nil {
		return err
	}
	return r.AwaitTermination()
}

// Relays is the ids of the relay instances bound on this runner now, read
// without the controller.
func (r *Runner) Relays() ([]string, error) {
	if !r.started.Load() {
		return nil, refusal("runtime.runner_not_started", nil)
	}
	answer, err := r.core.Relays()
	if err != nil {
		return nil, err
	}
	var ids []string
	err = json.Unmarshal(answer, &ids)
	return ids, err
}

// SendOption qualifies one Send.
type SendOption func(*sendOptions)

type sendOptions struct {
	msgID, key string
	local      bool
}

// MsgID stamps the message as flz.msg_id, so a relay whose input log still
// holds that id absorbs the repeat.
func MsgID(id string) SendOption { return func(o *sendOptions) { o.msgID = id } }

// SendKey sends to the partition group member whose range owns key.
func SendKey(key string) SendOption { return func(o *sendOptions) { o.key = key } }

// Local sends in process, without the controller: the message is on no disk,
// is lost if the runner stops before handling it, and the relay's input log
// does not cover it.
func Local() SendOption { return func(o *sendOptions) { o.local = true } }

// SendVerdict is what the controller did with a message: delivered, absorbed
// or suspended, on the relay it reached.
type SendVerdict struct {
	To      string `json:"to"`
	Verdict string `json:"verdict"`
}

// Send sends message to the entry channel of relayID, through the controller
// under the same role check as Control, answering its verdict; a Local send
// answers nil.
func (r *Runner) Send(relayID, channel string, message []byte, opts ...SendOption) (*SendVerdict, error) {
	var o sendOptions
	for _, opt := range opts {
		opt(&o)
	}
	answer, err := r.core.SendMessage(relayID, channel, message, o.msgID, o.key, o.local)
	if err != nil {
		return nil, err
	}
	var verdict *SendVerdict
	err = json.Unmarshal(answer, &verdict)
	return verdict, err
}

// Call calls the entry channel of relayID with value, through the controller
// under the same role check as Send, answering what the relay answers through
// its outputs: a JSON answer decoded, a text one as a string, a binary one as
// []byte, and a channel that does not answer as the controller's reply.
func (r *Runner) Call(relayID, channel string, value any) (any, error) {
	encoded, err := plain(value, "call of '"+channel+"'")
	if err != nil {
		return nil, err
	}
	reply, err := r.Control().Call("Relay.call_channel", relayID, channel,
		base64.StdEncoding.EncodeToString(encoded), map[string]any{"payload": "base64"})
	if err != nil {
		return nil, err
	}
	fields, ok := reply.(map[string]any)
	if !ok {
		return reply, nil
	}
	codec, ok := fields["answer_codec"].(string)
	if !ok {
		return reply, nil
	}
	if codec == "json" || codec == "text" {
		return fields["answer"], nil
	}
	answer, _ := fields["answer"].(string)
	return base64.StdEncoding.DecodeString(answer)
}

// Control administers the namespace this connection proved.
func (r *Runner) Control() *Control { return &Control{core: r.core} }
