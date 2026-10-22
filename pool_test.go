package flezha

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flezha/flezha_go_sdk/internal/native"
)

// fakeGovernor answers the pool as the core would, from a script: queued work
// is handed to whichever poller polls, and the governor's answers come from a
// channel the test feeds.
type fakeGovernor struct {
	work     chan []byte
	answers  chan int
	shutdown atomic.Bool
	left     atomic.Int32
}

func newFakeGovernor() *fakeGovernor {
	return &fakeGovernor{work: make(chan []byte, 64), answers: make(chan int, 64)}
}

func (g *fakeGovernor) Poll(timeoutMs uint64) (int, []byte, error) {
	if g.shutdown.Load() {
		return native.PollShuttingDown, nil, nil
	}
	select {
	case a := <-g.work:
		return native.PollActivation, a, nil
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		if g.shutdown.Load() {
			return native.PollShuttingDown, nil, nil
		}
		return native.PollTimeout, nil, nil
	}
}

func (g *fakeGovernor) PoolSupervise() (int, error) {
	select {
	case a := <-g.answers:
		return a, nil
	case <-time.After(20 * time.Millisecond):
		if g.shutdown.Load() {
			return native.PoolStop, nil
		}
		return native.PoolIdle, nil
	}
}

func (g *fakeGovernor) PoolLeft() error {
	g.left.Add(1)
	return nil
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testPool(g *fakeGovernor, base int, serve func([]byte)) *pool {
	return &pool{
		core:   g,
		policy: native.PoolPolicy{Base: base, Ceiling: 10 * base, PollTimeoutMs: 10, RetireMs: 60, DrainBoundMs: 500},
		serve:  serve, log: quietLog(),
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAWorkloadThatNeverStarvesStaysOnTheBasePool(t *testing.T) {
	g := newFakeGovernor()
	var served atomic.Int32
	p := testPool(g, 2, func([]byte) { served.Add(1) })
	done := make(chan struct{})
	go func() { p.run(); close(done) }()
	for range 40 {
		g.work <- []byte("a")
	}
	waitFor(t, "every activation served", func() bool { return served.Load() == 40 })
	if live := p.live.Load(); live != 2 {
		t.Fatalf("the pool is %d pollers, the base is 2", live)
	}
	g.shutdown.Store(true)
	<-done
	if p.live.Load() != 0 || g.left.Load() != 2 {
		t.Fatalf("after shutdown: %d live, %d seats given back", p.live.Load(), g.left.Load())
	}
}

func TestBlockedHandlersBuyWorkersAndGrownOnesRetire(t *testing.T) {
	g := newFakeGovernor()
	release := make(chan struct{})
	var served atomic.Int32
	p := testPool(g, 1, func(a []byte) {
		if string(a) == "block" {
			<-release
		}
		served.Add(1)
	})
	done := make(chan struct{})
	go func() { p.run(); close(done) }()

	// The one poller is held inside a handler with work still queued: the
	// governor answers grown seats, and the queued work is served by them.
	g.work <- []byte("block")
	waitFor(t, "the base poller to be held", func() bool { return len(g.work) == 0 })
	for range 5 {
		g.work <- []byte("queued")
	}
	for range 3 {
		g.answers <- native.PoolSpawnGrown
	}
	waitFor(t, "the queued work served by grown pollers", func() bool { return served.Load() == 5 })
	if live := p.live.Load(); live != 4 {
		t.Fatalf("the pool grew to %d pollers, want the base plus three", live)
	}

	// The blocking workload ends; the grown pollers idle past the retire time
	// and leave, giving their seats back, and the pool is the base again.
	close(release)
	waitFor(t, "the grown pollers to retire", func() bool { return p.live.Load() == 1 })
	if g.left.Load() != 3 {
		t.Fatalf("%d seats given back, want the three grown ones", g.left.Load())
	}
	g.shutdown.Store(true)
	<-done
}

func TestABasePollerThatLeftIsReplaced(t *testing.T) {
	g := newFakeGovernor()
	p := testPool(g, 1, func([]byte) {})
	done := make(chan struct{})
	go func() { p.run(); close(done) }()
	g.answers <- native.PoolSpawnBase
	waitFor(t, "the replacement", func() bool { return p.live.Load() == 2 })
	g.shutdown.Store(true)
	<-done
}

func TestAPollerHeldPastTheDrainBoundLeavesTheContextUnfreed(t *testing.T) {
	g := newFakeGovernor()
	hold := make(chan struct{})
	defer close(hold)
	var once sync.Once
	entered := make(chan struct{})
	p := testPool(g, 1, func([]byte) {
		once.Do(func() { close(entered) })
		<-hold
	})
	p.policy.DrainBoundMs = 100
	done := make(chan struct{})
	go func() { p.run(); close(done) }()
	g.work <- []byte("forever")
	<-entered
	g.shutdown.Store(true)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the pool waited past its drain bound")
	}
	if !p.stuck.Load() {
		t.Fatal("a poller held inside a tasklet was not reported")
	}
}
