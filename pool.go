package flezha

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flezha/flezha_go_sdk/internal/native"
)

// poolCore is what the pool asks the core: an activation, and the governor's
// next decision.
type poolCore interface {
	Poll(timeoutMs uint64) (int, []byte, error)
	PoolSupervise() (int, error)
	PoolLeft() error
}

// pool is the runner's pollers: a base of goroutines each looping poll,
// dispatch and complete, grown on the core governor's word when every poller
// is held inside a tasklet (a completion blocked on a full DropNothing
// consumer is one, and only a new poller drains it). No goroutine is locked to
// its thread: the core takes a completion from any thread.
type pool struct {
	core     poolCore
	policy   native.PoolPolicy
	serve    func(activation []byte)
	log      *slog.Logger
	onAnswer func()

	pollers sync.WaitGroup
	// The pollers alive now.
	live atomic.Int32
	// Pollers were still inside a tasklet when the drain bound ran out; the
	// context is not freed under them.
	stuck atomic.Bool
}

// run starts the base and answers the governor until the pool is over, then
// waits for the pollers up to the governor's drain bound.
func (p *pool) run() {
	for range p.policy.Base {
		p.spawn(false)
	}
	p.supervise()
	p.awaitPollers(time.Duration(p.policy.DrainBoundMs) * time.Millisecond)
}

// supervise answers the governor: one decision per wait, at most a second apart.
func (p *pool) supervise() {
	for {
		answer, err := p.core.PoolSupervise()
		if p.onAnswer != nil {
			p.onAnswer()
		}
		if err != nil {
			p.log.Error("the pool supervisor failed", "error", err)
			return
		}
		switch answer {
		case native.PoolStop:
			return
		case native.PoolSpawnBase:
			p.spawn(false)
		case native.PoolSpawnGrown:
			p.spawn(true)
		}
	}
}

// spawn starts one poller in a seat the governor reserved. A grown poller
// leaves after the governor's retire time without work; every poller gives
// its seat back as it leaves.
func (p *pool) spawn(grown bool) {
	p.pollers.Add(1)
	p.live.Add(1)
	go func() {
		defer func() {
			p.live.Add(-1)
			_ = p.core.PoolLeft()
			p.pollers.Done()
		}()
		wait := time.Duration(p.policy.PollTimeoutMs) * time.Millisecond
		retire := time.Duration(p.policy.RetireMs) * time.Millisecond
		var idle time.Duration
		for {
			status, activation, err := p.core.Poll(uint64(p.policy.PollTimeoutMs))
			switch {
			case err != nil:
				p.log.Error("polling failed", "error", err)
				return
			case status == native.PollShuttingDown:
				return
			case status == native.PollActivation:
				idle = 0
				p.serve(activation)
			default:
				idle += wait
				if grown && idle >= retire {
					return
				}
			}
		}
	}()
}

// awaitPollers waits for every poller to leave, up to bound: a tasklet that
// never returns holds its poller forever, and a runner stopped by closing its
// pipes must exit rather than wait on it.
func (p *pool) awaitPollers(bound time.Duration) {
	left := make(chan struct{})
	go func() {
		p.pollers.Wait()
		close(left)
	}()
	select {
	case <-left:
	case <-time.After(bound):
		p.stuck.Store(true)
		p.log.Warn(fmt.Sprintf("%d poller(s) were still inside a tasklet after the %s drain; the runner "+
			"is leaving anyway, and the core re-hands their unanswered work on the next bind", p.live.Load(), bound))
	}
}
