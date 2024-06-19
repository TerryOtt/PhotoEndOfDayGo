package main

import (
	"fmt"
	"time"
)
import "github.com/google/uuid"

type timerInFlight struct {
	operationDescription string
	startTime            time.Time
}

type CompletedTimer struct {
	operationDescription string
	duration             time.Duration
}

type PerfTimer struct {
	timersInFlight  map[uuid.UUID]timerInFlight
	completedTimers []CompletedTimer
	cumulativeTime  *time.Duration
}

type PerfTimingInfo struct {
	operationTimes []CompletedTimer
	cumulativeTime time.Duration
}

func NewPerfTimer() *PerfTimer {
	return &PerfTimer{
		make(map[uuid.UUID]timerInFlight),
		[]CompletedTimer{},
		nil,
	}
}

func (pf *PerfTimer) enterFunction(opDescription string) uuid.UUID {
	timerId := uuid.New()
	pf.timersInFlight[timerId] = timerInFlight{opDescription, time.Now()}
	//fmt.Printf("Started new timer for operation \"%s\", ID: %s\n", opDescription, timerId.String())
	return timerId
}

func (pf *PerfTimer) exitFunction(timerId uuid.UUID) {
	val, ok := pf.timersInFlight[timerId]
	if !ok {
		panic("Called exitFunction on ID we are not tracking: " + timerId.String())
	}

	opDuration := time.Since(val.startTime)

	newCompletedTimer := CompletedTimer{
		val.operationDescription,
		opDuration,
	}

	pf.completedTimers = append(pf.completedTimers, newCompletedTimer)

	// Delete the timer from the list of in flight timers to avoid incorrect interface usage
	delete(pf.timersInFlight, timerId)

	fmt.Printf("Just completed timer for operation \"%s\" with time %s\n",
		val.operationDescription, newCompletedTimer.duration)
	if pf.cumulativeTime == nil {
		pf.cumulativeTime = &opDuration
	} else {
		*pf.cumulativeTime += opDuration
	}

	fmt.Printf("Cumulative time is now %s\n", pf.cumulativeTime.String())
}

func (pf *PerfTimer) PerformanceStats() PerfTimingInfo {

	return PerfTimingInfo{
		pf.completedTimers,
		*pf.cumulativeTime,
	}
}
