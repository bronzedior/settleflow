package queue

import (
	"fmt"
	"sync"
)

type SideEffectKey struct {
	Key string
}

type SideEffectRecorder struct {
	mu      sync.RWMutex
	effects map[string]int
}

func NewSideEffectRecorder() *SideEffectRecorder {
	return &SideEffectRecorder{
		effects: make(map[string]int),
	}
}

func (r *SideEffectRecorder) Record(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.effects[key]++
}

func (r *SideEffectRecorder) Count(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.effects[key]
}

func (r *SideEffectRecorder) AllEffects() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]int)
	for k, v := range r.effects {
		result[k] = v
	}
	return result
}

func (r *SideEffectRecorder) AssertExactlyOnce(key string) error {
	count := r.Count(key)
	if count != 1 {
		return fmt.Errorf("side effect %q occurred %d times, expected 1", key, count)
	}
	return nil
}

func (r *SideEffectRecorder) AssertAtLeastOnce(key string) error {
	count := r.Count(key)
	if count < 1 {
		return fmt.Errorf("side effect %q occurred %d times, expected at least 1", key, count)
	}
	return nil
}

func (r *SideEffectRecorder) AssertAtMostOnce(key string) error {
	count := r.Count(key)
	if count > 1 {
		return fmt.Errorf("side effect %q occurred %d times, expected at most 1", key, count)
	}
	return nil
}
