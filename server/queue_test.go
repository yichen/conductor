package server

import (
	"strconv"
	"testing"
)

func TestQueue(t *testing.T) {
	t.Parallel()

	q := NewQueue(100)

	for i := 1; i <= 10; i++ {
		q.Offer(Job{
			Workflow: "wf1",
			Name:     strconv.Itoa(i),
			Data:     "aaa",
		})
	}

	// send duplicates
	for i := 1; i <= 10; i++ {
		q.Offer(Job{
			Workflow: "wf1",
			Name:     strconv.Itoa(i),
			Data:     "aaa",
		})
	}

	for i := 1; i <= 10; i++ {
		j := q.Poll()

		if j.Name != strconv.Itoa(i) {
			t.Errorf("expected ID %d, actual: %s", i, j.Name)
		}
	}
}

func TestQueueDedup(t *testing.T) {
	t.Parallel()

	q := NewQueue(100)

	for i := 1; i <= 10; i++ {
		q.Offer(Job{
			Workflow: "wf1",
			Name:     "aaa",
			Data:     "aaa",
		})
	}

	if q.size != 1 {
		t.Errorf("expected queue size: 1, actual: %d", q.size)
	}
}
