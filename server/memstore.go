package server

import "sync"

// MemStore stores the current live job queues. It also allows the consumer to
// poll one job that is not being processed.
type MemStore struct {
	lock sync.RWMutex

	size   int
	queues map[string]map[string]*Queue
}

// NewMemStore creats a new instance of MemStore
func NewMemStore(size int) *MemStore {
	return &MemStore{
		size:   size,
		queues: make(map[string]map[string]*Queue),
	}
}

// Offer adds a new job to the mem store
func (m *MemStore) Offer(workflow string, state string, job Job) {
	if _, ok := m.queues[workflow]; !ok {
		m.lock.Lock()
		m.queues[workflow] = make(map[string]*Queue)
		m.lock.Unlock()
	}

	if _, ok := m.queues[workflow][state]; !ok {
		m.lock.Lock()
		m.queues[workflow][state] = NewQueue(m.size)
		m.lock.Unlock()
	}

	q := m.queues[workflow][state]
	q.Offer(job)
}

// Poll returns a job if it exists in the store for a workflow/state combination
func (m *MemStore) Poll(workflow string, state string) *Job {
	if _, ok := m.queues[workflow]; !ok {
		return nil
	}

	if _, ok := m.queues[workflow][state]; !ok {
		return nil
	}

	q := m.queues[workflow][state]
	if q.size == 0 {
		return nil
	}

	j := q.Poll()
	return &j
}
