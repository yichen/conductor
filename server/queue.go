package server

import (
	"fmt"
	"sync"
	"time"
)

// Queue holds a dedup job queue for a specific workflow/state
type Queue struct {
	sync.RWMutex

	size        int
	jobMap      map[string]Job
	hiddenJobAt map[string]time.Time
	jobC        chan string
	hiddenJobC  chan string
}

// NewQueue creates a new Queue with dedup
func NewQueue(size int) *Queue {
	q := Queue{
		jobMap:      make(map[string]Job),
		jobC:        make(chan string, size),
		hiddenJobAt: make(map[string]time.Time),
		hiddenJobC:  make(chan string, size),
	}

	return &q
}

// Offer a new job to the dedup job queue. Each
// job is identified by a unique key of the format
// {workflow}:{job}, so that the same job will be
// send to the same server instance.
func (q *Queue) Offer(job Job) {
	k := fmt.Sprintf("%s:%s", job.GetWorkflow(), job.GetName())

	q.Lock()
	defer q.Unlock()

	if _, ok := q.jobMap[k]; !ok {
		q.jobC <- k
		q.size++
	}
	q.jobMap[k] = job
}

// Poll returns a job and hide it from the queue.
func (q *Queue) Poll() Job {
	k := <-q.jobC

	// find the first job that is not deleted
	for {
		q.RLock()
		_, ok := q.jobMap[k]
		q.RUnlock()

		if !ok {
			k = <-q.jobC
		} else {
			break
		}
	}

	q.hiddenJobC <- k
	q.hiddenJobAt[k] = time.Now()

	q.Lock()
	r := q.jobMap[k]
	delete(q.jobMap, k)
	q.Unlock()

	return r
}

// Remove removes the job from the queue
func (q *Queue) Remove(job Job) {
	k := fmt.Sprintf("%s:%s", job.GetWorkflow(), job.GetName())

	q.Lock()
	defer q.Unlock()

	delete(q.jobMap, k)
}

// Peak peaks a job without removing it from the queue
func (q *Queue) Peak() Job {
	k := <-q.jobC
	q.jobC <- k

	q.Lock()
	r := q.jobMap[k]
	q.Unlock()

	return r
}

// PeakNext peaks a job without removing it from the queue
func (q *Queue) PeakNext() Job {
	k := <-q.jobC
	q.jobC <- k

	q.Lock()
	r := q.jobMap[k]
	delete(q.jobMap, k)
	q.Unlock()

	return r

}

// Size returns the current size of the queue
func (q *Queue) Size() int {
	q.RLock()
	defer q.RUnlock()

	return q.size
}
