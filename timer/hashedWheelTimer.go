package timer

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
	"math"
)

type cancelledtask struct {
	run func()
}

// wheel timer status
const (

	WORKER_STATE_INIT = iota
	WORKER_STATE_STARTED
	WORKER_STATE_SHUTDOWN
)

type HashedWheelTimer struct {
	startTime    int64
	workerState  int32
	tickDuration int64
	wheel        []hashedWheelBucket
	mask         int

	timeouts            Queue
	cancelledTimeouts   Queue
	unprocessedTimeouts Queue
}

func NewTimer() *HashedWheelTimer {
	timer := new(HashedWheelTimer);
	
	timeouts            := NewQueue();
	cancelledTimeouts   := NewQueue();
	unprocessedTimeouts := NewQueue();
	
	timer.timeouts 				= timeouts;
	timer.cancelledTimeouts 	= cancelledTimeouts;
	timer.unprocessedTimeouts 	= unprocessedTimeouts;
	
	return timer;
}

func (this *HashedWheelTimer) start() (err error) {
	switch atomic.LoadInt32(&this.workerState) {
	case WORKER_STATE_INIT:
		if atomic.CompareAndSwapInt32(&this.workerState, WORKER_STATE_INIT, WORKER_STATE_STARTED) {
			go this.daemon(0)
		}
		break
	case WORKER_STATE_STARTED:
		break
	case WORKER_STATE_SHUTDOWN:
		err = errors.New("cannot be started once stopped")
	default:
		err = errors.New("Invalid WorkerState")
	}

	// Wait until the startTime is initialized by the worker.
	for this.startTime == 0 {
		//           startTimeInitialized.await();
	}
	return
}

// hashed wheel timer daemon go
func (this *HashedWheelTimer) daemon(tick int64) func() {
	return func() {
		// Initialize the startTime.
		this.startTime = time.Now().UnixNano()
		if this.startTime == 0 {
			// We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
			this.startTime = 1
		}

		// Notify the other threads waiting for the initialization at start().
		//            startTimeInitialized.countDown();

		for (atomic.LoadInt32(&this.workerState)) == WORKER_STATE_STARTED {
			deadline := waitForNextTick(this.tickDuration, this.startTime, tick)
			if deadline > 0 {
				idx := (tick & int64(this.mask))
				processCancelledTasks(this.cancelledTimeouts)
				bucket := this.wheel[idx]
				transferTimeoutsToBuckets(this.timeouts, this.tickDuration, tick, this)
				bucket.expireTimeouts(deadline)
				tick++
			}
		}

		// Fill the unprocessedTimeouts so we can return them from stop() method.
		for _, bucket := range this.wheel {
			bucket.clearTimeouts(this.unprocessedTimeouts)
		}

		for {
			e := this.timeouts.poll()
			if e == nil {
				break
			}

			if timeout, ok := e.(hashedWheelTimeout); ok {
				if !timeout.isCancelled() {
					this.unprocessedTimeouts.add(timeout)
				}
			}

		}
		processCancelledTasks(this.cancelledTimeouts)
	}
}

func waitForNextTick(tickDuration, startTime, tick int64) int64 {

	deadline := tickDuration * (tick + 1)

	for {
		currentTime := time.Now().UnixNano() - startTime
		sleepTimeMs := (deadline - currentTime + 999999) / 1000000

		if sleepTimeMs <= 0 {
			if currentTime == math.MinInt64 {
				return -(math.MaxInt64)
			} else {
				return currentTime
			}
		}

		time.Sleep(time.Millisecond * time.Duration(sleepTimeMs))

	}
}

// sigle comsure
func processCancelledTasks(cancelledTimeouts Queue) {
	for {
		task := cancelledTimeouts.poll()
		if task == nil {
			// all processed
			break
		}

		if timeout, ok := task.(cancelledtask); ok {
			/// ???
			go timeout.run()
//			timeout.run()
		}
	}
}

func transferTimeoutsToBuckets(timeouts Queue, tickDuration, tick int64, timer *HashedWheelTimer) {

	var (
		calculated int64
		ticks      int64
	)

	// transfer only max. 100000 timeouts per tick to prevent a thread to stale the workerThread when it just
	// adds new timeouts in a loop.
	for i := 0; i < 100000; i++ {
		e := timeouts.poll()

		if e == nil {
			// all processed
			break
		}

		if timeout, ok := e.(hashedWheelTimeout); ok {

			if timeout.state == ST_CANCELLED {
				// Was cancelled in the meantime.
				continue
			}

			calculated = timeout.deadline / tickDuration
			timeout.remainingRounds = (calculated - tick) / int64(len(timer.wheel))

			if calculated > tick {
				ticks = calculated
			} else {
				ticks = tick
			} // Ensure we don't schedule for past.

			stopIndex := (int(ticks) & timer.mask)
			//
			bucket := timer.wheel[stopIndex]
			bucket.addTimeout(&timeout)
		}
	}
}

type hashedWheelBucket struct {
	head *hashedWheelTimeout
	tail *hashedWheelTimeout
}

func (this *hashedWheelBucket) addTimeout(timeout *hashedWheelTimeout) (err error) {
	if timeout.bucket == nil {
		err = errors.New("add timeout to bucket is nil")
	}
	timeout.bucket = this
	if this.head == nil {
		this.tail = timeout
		this.head = timeout
	} else {
		this.tail.next = timeout
		this.tail = timeout
	}
	return
}

func (this *hashedWheelBucket) expireTimeouts(deadline int64) (err error) {
	timeout := this.head

	// process all timeouts
	for timeout != nil {
		remove := false
		if timeout.remainingRounds <= 0 {
			if timeout.deadline <= deadline {
				timeout.expire()
			} else {
				// The timeout was placed into a wrong slot. This should never happen.
				err = errors.New(fmt.Sprintf("timeout.deadline (%d) > deadline (%d)", timeout.deadline, deadline))
			}
			remove = true
		} else if timeout.isCancelled() {
			remove = true
		} else {
			timeout.remainingRounds--
		}
		// store reference to next as we may null out timeout.next in the remove block.
		next := timeout.next
		if remove {
			this.remove(timeout)
		}
		timeout = (*hashedWheelTimeout)(next)
	}
	return
}

func (this *hashedWheelBucket) remove(timeout *hashedWheelTimeout) {
	next := timeout.next
	// remove timeout that was either processed or cancelled by updating the linked-list
	if timeout.prev != nil {
		((*hashedWheelTimeout)(timeout.prev)).next = next
	}
	if timeout.next != nil {
		((*hashedWheelTimeout)(timeout.next)).prev = timeout.prev
	}

	if timeout == this.head {
		// if timeout is also the tail we need to adjust the entry too
		if timeout == this.tail {
			this.tail = nil
			this.head = nil
		} else {
			this.head = (*hashedWheelTimeout)(next)
		}
	} else if timeout == this.tail {
		// if the timeout is the tail modify the tail to be the prev node.
		this.tail = (*hashedWheelTimeout)(timeout.prev)
	}
	// null out prev, next and bucket to allow for GC.
	timeout.prev = nil
	timeout.next = nil
	timeout.bucket = nil
}

func (this *hashedWheelBucket) pollTimeout() *hashedWheelTimeout {
	head := this.head
	if head == nil {
		return nil
	}
	next := head.next
	if next == nil {
		this.tail = nil
		this.head = nil
	} else {
		this.head = (*hashedWheelTimeout)(next)
		((*hashedWheelTimeout)(next)).prev = nil
	}

	// nil out prev and next to allow for GC.
	head.next = nil
	head.prev = nil
	head.bucket = nil
	return head
}

func (this *hashedWheelBucket) clearTimeouts(set Queue) {
	for {
		timeout := this.pollTimeout()
		if timeout == nil {
			return
		}
		if timeout.isExpired() || timeout.isCancelled() {
			continue
		}
		set.add(timeout)
	}
}

const (
	ST_INIT = iota
	ST_CANCELLED
	ST_EXPIRED
)

type hashedWheelTimeout struct {
	state int32

	deadline        int64
	remainingRounds int64

	bucket *hashedWheelBucket

	next *hashedWheelTimeout
	prev *hashedWheelTimeout

	// 闭包
	task func()
}

func (this *hashedWheelTimeout) newHashedWheelTimeout(timer HashedWheelTimer, deadline int64, task func()) {
	//            this.timer = timer;
	this.task = task
	this.deadline = deadline
}

func (this *hashedWheelTimeout) compareAndSetState(expected int32, state int32) bool {
	return atomic.CompareAndSwapInt32(&this.state, expected, state)
}

func (this *hashedWheelTimeout) isCancelled() bool {
	return this.state == ST_CANCELLED
}

func (this *hashedWheelTimeout) isExpired() bool {
	return this.state == ST_EXPIRED
}

func (this *hashedWheelTimeout) expire() {
	if !this.compareAndSetState(ST_INIT, ST_EXPIRED) {
		return
	}
	this.task()
	//go this.task()
}

func (this *hashedWheelTimeout) cancel(timer *HashedWheelTimer) bool {
	// only update the state it will be removed from HashedWheelBucket on next tick.
	if !this.compareAndSetState(ST_INIT, ST_CANCELLED) {
		return false
	}
	// If a task should be canceled we create a new Runnable for this to another queue which will
	// be processed on each tick. So this means that we will have a GC latency of max. 1 tick duration
	// which is good enough. This way we can make again use of our MpscLinkedQueue and so minimize the
	// locking / overhead as much as possible.
	//
	// It is important that we not just add the HashedWheelTimeout itself again as it extends
	// MpscLinkedQueueNode and so may still be used as tombstone.
	if this.bucket != nil {
		
		caneltask := func(bucket *hashedWheelBucket, timeout *hashedWheelTimeout) func() {
			return func() {
				if bucket != nil {
					bucket.remove(timeout)
				}
			}
		}(this.bucket, this)
	
		timer.cancelledTimeouts.add(cancelledtask{run: caneltask})
		
	}
	return true
}
