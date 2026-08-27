package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bronzedior/settleflow/internal/lifecycle"
)

type JobMeta struct {
	ID          JobID
	Attempt     int
	MaxAttempts int
	Checkpoint  []byte
	EnqueuedAt  time.Time
}

type Handler func(ctx context.Context, meta JobMeta) error

type PoolConfig struct {
	WorkerID          string
	Queues            []string
	Concurrency       int
	MaxBatch          int
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	ReaperThreshold   time.Duration
	JobTimeout        time.Duration
	StatementTimeout  time.Duration
	IdleInTxnTimeout  time.Duration
	Logger            *slog.Logger
}

type Pool struct {
	lifecycle.ComponentBase
	config *PoolConfig
	store  *Store

	baseCtx   context.Context
	drainCh   chan struct{}
	doneOnce  sync.Once
	drainOnce sync.Once

	freeSlotsmu sync.Mutex
	freeSlots   int

	activemu     sync.Mutex
	activeJobs   map[JobID]*ActiveJob
	drainTimeout time.Duration
}

type ActiveJob struct {
	Mu            sync.Mutex
	Meta          JobMeta
	ProgressMu    sync.Mutex
	LastProgress  []byte
	IsDraining    bool
	CancelContext context.CancelFunc
}

func NewPool(store *Store, config *PoolConfig) *Pool {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	return &Pool{
		ComponentBase: lifecycle.NewComponentBase("pool"),
		config:        config,
		store:         store,
		drainCh:       make(chan struct{}),
		freeSlots:     config.Concurrency,
		activeJobs:    make(map[JobID]*ActiveJob),
		drainTimeout:  30 * time.Second,
	}
}

func (p *Pool) Start(ctx context.Context) error {
	p.baseCtx = ctx
	p.config.Logger.Info("Pool started", "workerID", p.config.WorkerID, "concurrency", p.config.Concurrency)
	return nil
}

func (p *Pool) Stop(ctx context.Context) error {
	p.config.Logger.Info("Pool stopping")
	p.Drain()
	return p.Wait(ctx)
}

func (p *Pool) Drain() {
	p.drainOnce.Do(func() {
		p.config.Logger.Info("Drain initiated")
		close(p.drainCh)
	})
}

func (p *Pool) Draining() bool {
	select {
	case <-p.drainCh:
		return true
	default:
		return false
	}
}

func (p *Pool) Wait(ctx context.Context) error {
	p.config.Logger.Info("Pool waiting for in-flight jobs to complete")

	deadline := time.Now().Add(p.drainTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		p.activemu.Lock()
		count := len(p.activeJobs)
		p.activemu.Unlock()

		if count == 0 {
			p.config.Logger.Info("All jobs completed during drain")
			return nil
		}

		select {
		case <-ctx.Done():
			p.activemu.Lock()
			remaining := len(p.activeJobs)
			p.activemu.Unlock()
			if remaining > 0 {
				p.config.Logger.Warn("Drain context cancelled with jobs still running", "count", remaining)
			}
			return ctx.Err()
		case <-time.After(time.Until(deadline)):
			p.activemu.Lock()
			remaining := len(p.activeJobs)
			p.activemu.Unlock()
			if remaining > 0 {
				p.config.Logger.Warn("Drain timeout with jobs still running", "count", remaining)
			}
			return nil
		case <-ticker.C:
			p.activemu.Lock()
			count := len(p.activeJobs)
			p.activemu.Unlock()
			if count > 0 {
				p.config.Logger.Debug("Waiting for jobs to complete", "count", count)
			}
		}
	}
}

func (p *Pool) AcquireSlot(ctx context.Context) bool {
	p.freeSlotsmu.Lock()
	defer p.freeSlotsmu.Unlock()

	if p.freeSlots > 0 {
		p.freeSlots--
		return true
	}
	return false
}

func (p *Pool) ReleaseSlot() {
	p.freeSlotsmu.Lock()
	defer p.freeSlotsmu.Unlock()
	p.freeSlots++
}

func (p *Pool) RegisterActive(id JobID, meta JobMeta) *ActiveJob {
	p.activemu.Lock()
	defer p.activemu.Unlock()

	aj := &ActiveJob{
		Meta: meta,
	}
	p.activeJobs[id] = aj
	return aj
}

func (p *Pool) UnregisterActive(id JobID) {
	p.activemu.Lock()
	defer p.activemu.Unlock()
	delete(p.activeJobs, id)
}

func (p *Pool) SetProgress(jobID JobID, checkpoint []byte) error {
	p.activemu.Lock()
	defer p.activemu.Unlock()

	aj, ok := p.activeJobs[jobID]
	if !ok {
		return fmt.Errorf("job not active: %s", jobID)
	}

	aj.ProgressMu.Lock()
	aj.LastProgress = checkpoint
	aj.ProgressMu.Unlock()

	return nil
}

func (p *Pool) JobFromContext(ctx context.Context) JobMeta {
	if meta, ok := ctx.Value(jobMetaKey).(JobMeta); ok {
		return meta
	}
	return JobMeta{}
}

func (p *Pool) IsDraining(ctx context.Context) bool {
	if isDraining, ok := ctx.Value(isDrainingKey).(bool); ok {
		return isDraining
	}
	return false
}

var jobMetaKey = struct{}{}
var isDrainingKey = struct{}{}

func (p *Pool) makeJobContext(meta JobMeta) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(p.baseCtx, p.config.JobTimeout)
	ctx = context.WithValue(ctx, jobMetaKey, meta)
	ctx = context.WithValue(ctx, isDrainingKey, false)
	return ctx, cancel
}

func (p *Pool) Claim() ([]Job, error) {
	p.freeSlotsmu.Lock()
	k := p.freeSlots
	if k > p.config.MaxBatch {
		k = p.config.MaxBatch
	}
	p.freeSlotsmu.Unlock()

	if k == 0 {
		return nil, nil
	}

	return p.store.ClaimJobs(p.baseCtx, p.config.WorkerID, p.config.Queues, k)
}

func (p *Pool) GetActiveJobsForHeartbeat() ([]JobID, [][]byte) {
	p.activemu.Lock()
	defer p.activemu.Unlock()

	ids := make([]JobID, 0, len(p.activeJobs))
	checkpoints := make([][]byte, 0, len(p.activeJobs))

	for id, aj := range p.activeJobs {
		aj.ProgressMu.Lock()
		cp := aj.LastProgress
		aj.ProgressMu.Unlock()

		ids = append(ids, id)
		checkpoints = append(checkpoints, cp)
	}

	return ids, checkpoints
}

func (p *Pool) BaseContext() context.Context {
	return p.baseCtx
}

func (p *Pool) Store() *Store {
	return p.store
}

func (p *Pool) Config() *PoolConfig {
	return p.config
}

func (p *Pool) DrainChannel() <-chan struct{} {
	return p.drainCh
}
