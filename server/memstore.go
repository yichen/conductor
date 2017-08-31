package server

import (
	"fmt"
	"sync"
)

// MemStore stores the current live job queues. It also allows the consumer to
// poll one job that is not being processed.
type MemStore struct {
	sync.RWMutex
	// checkpoint is the current key in the WAL
	checkpoint string
	// size is the capacity of each queue
	size int
	// queues is a set of in-memory queues that is identified by workfow-state
	// so that a client can
	queues map[string]map[string]*Queue
	// map from a job identified by {workflow}-{name}, to the current
	// state of the job.
	jobStateMap map[string]string
}

// NewMemStore creats a new instance of MemStore
func NewMemStore(size int) *MemStore {
	return &MemStore{
		size:        size,
		queues:      make(map[string]map[string]*Queue),
		jobStateMap: make(map[string]string),
	}
}

// Start will start the memstore service, which will read the WAL,
// remove older entry when nessessary.
func (m *MemStore) Start() {

}

// Offer adds a new job to the mem store. If the job already exists with
// a different state
func (m *MemStore) Offer(workflow string, state string, job Job) {
	if _, ok := m.queues[workflow]; !ok {
		m.Lock()
		m.queues[workflow] = make(map[string]*Queue)
		m.Unlock()
	}

	if _, ok := m.queues[workflow][state]; !ok {
		m.Lock()
		m.queues[workflow][state] = NewQueue(m.size)
		m.Unlock()
	}

	q := m.queues[workflow][state]
	q.Offer(job)

	key := fmt.Sprintf("%s:%s", workflow, job.Name)
	m.Lock()
	m.jobStateMap[key] = state
	m.Unlock()
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

// Stop stops the memstore service. It is called when the server is
// gracefully shutdown.
func (m *MemStore) Stop() {
	m.checkpoint = ""
	m.queues = make(map[string]map[string]*Queue)
	m.jobStateMap = make(map[string]string)
}
