package jobs

import (
	"github.com/google/uuid"
	"github.com/riverqueue/river"
)

const QueueDiscovery = "discovery"

type DiscoverPageArgs struct {
	ScanRunID  uuid.UUID `json:"scan_run_id"`
	Resource   string    `json:"resource"` 
	ObjectKind string    `json:"kind"`     
	NextLink   string    `json:"next_link"`
}

func (DiscoverPageArgs) Kind() string { return "discover.page" }

func (DiscoverPageArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueDiscovery, MaxAttempts: 10}
}

type DiscoverDetailArgs struct {
	ScanRunID  uuid.UUID `json:"scan_run_id"`
	Resource   string    `json:"resource"`
	ObjectKind string    `json:"kind"`
	ObjectID   string    `json:"object_id"`
}

func (DiscoverDetailArgs) Kind() string { return "discover.detail" }

func (DiscoverDetailArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueDiscovery, MaxAttempts: 10}
}

type EvaluateScanRunArgs struct {
	ScanRunID uuid.UUID `json:"scan_run_id"`
}

func (EvaluateScanRunArgs) Kind() string { return "evaluate.scan_run" }

func (EvaluateScanRunArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueDiscovery, MaxAttempts: 40}
}
