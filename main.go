package main

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/semaphore"
)

type Result[T any] struct {
	Result *T
	Error  error
}

type Pool[T any] struct {
	Jobs            []func() (*T, error)
	MaxWorkers      uint
	running         bool
	results         []Result[T]
	results_chan    chan Result[T]
	panics          []interface{}
	panicC          chan interface{}
	workerSemaphore *semaphore.Weighted
	workerGroup     sync.WaitGroup
}

// Create easy to compare errors for this pool
var (
	ErrAlreadyRunning     = errors.New("pool is running")
	ErrAcquiringSemaphore = errors.New("failed to acquire semaphore")
)

// Create a new generic pool with a given size
func NewPool[T any](jobs, workers uint) *Pool[T] {
	return &Pool[T]{
		Jobs:            make([]func() (*T, error), 0, jobs),
		MaxWorkers:      workers,
		results:         make([]Result[T], 0, jobs),
		results_chan:    make(chan Result[T], jobs),
		panics:          make([]interface{}, 0, jobs),
		panicC:          make(chan interface{}, jobs),
		workerSemaphore: semaphore.NewWeighted(int64(workers)),
	}
}

// Add a job to the pool
func (p *Pool[T]) AddJob(job func() (*T, error)) error {
	if p.running {
		return ErrAlreadyRunning
	}
	p.Jobs = append(p.Jobs, job)
	return nil
}

func (p *Pool[T]) RunJob(job func() (*T, error)) {
	defer func() {
		if r := recover(); r != nil {
			p.panicC <- r
		}
		p.workerSemaphore.Release(1)
		p.workerGroup.Done()
	}()
	result, err := job()
	p.results_chan <- Result[T]{Result: result, Error: err}
}

// Run the pool
func (p *Pool[T]) Run() {
	p.running = true
	// Also take semaphore into account
	for i := range p.Jobs {
		if err := p.workerSemaphore.Acquire(context.Background(), 1); err != nil {
			// Add to failed jobs
			p.results_chan <- Result[T]{Error: ErrAcquiringSemaphore}
			continue
		}
		p.workerGroup.Add(1)
		go p.RunJob(p.Jobs[i])

	}
}

// Wait for the pool to finish
func (p *Pool[T]) Wait() (results []Result[T], panics []interface{}) {
	p.Run()
	p.workerGroup.Wait()
	close(p.results_chan)
	close(p.panicC)
	for result := range p.results_chan {
		p.results = append(p.results, result)
	}
	for panic := range p.panicC {
		p.panics = append(p.panics, panic)
	}
	return p.results, p.panics
}
