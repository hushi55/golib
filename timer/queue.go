package timer

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"
)

type Queue interface {
	poll() (e interface{})
	peek() (e interface{})

	offer(e interface{}) (ok bool, err error)
	add(e interface{}) (ok bool, err error)
}

func NewQueue() Queue {
	return new(mpscLinkedQueue)
}

//A lock-free concurrent single-consumer multi-producer Queue.
type mpscLinkedQueue struct {
	p00, p01, p02, p03, p04, p05, p06, p07 int64

	headRef *mpscLinkedQueueNode

	p10, p11, p12, p13, p14, p15, p16, p17 int64

	tailRef *mpscLinkedQueueNode

	p20, p21, p22, p23, p24, p25, p26, p27 int64
}

func (q *mpscLinkedQueue) PreventCompileroptimization() {
	fmt.Printf("padding is  %d, %d, %d, %d, %d, %d, %d,", q.p00, q.p01, q.p02, q.p03, q.p04, q.p05, q.p06, q.p07)
	fmt.Printf("padding is  %d, %d, %d, %d, %d, %d, %d,", q.p10, q.p11, q.p12, q.p13, q.p14, q.p15, q.p16, q.p17)
	fmt.Printf("padding is  %d, %d, %d, %d, %d, %d, %d,", q.p20, q.p21, q.p22, q.p23, q.p24, q.p25, q.p26, q.p27)
}

type mpscLinkedQueueNode struct {
	p00, p01, p02, p03, p04, p05, p06, p07 int64

	next *mpscLinkedQueueNode

	p30, p31, p32, p33, p34, p35, p36, p37 int64

	value interface{}
}

func (qn *mpscLinkedQueueNode) PreventCompileroptimization() {
	fmt.Printf("padding is  %d, %d, %d, %d, %d, %d, %d,", qn.p00, qn.p01, qn.p02, qn.p03, qn.p04, qn.p05, qn.p06, qn.p07)
	fmt.Printf("padding is  %d, %d, %d, %d, %d, %d, %d,", qn.p30, qn.p31, qn.p32, qn.p33, qn.p34, qn.p35, qn.p36, qn.p37)
}

func (this *mpscLinkedQueueNode) getValue() (v interface{}) {
	return this.value
}

func (this *mpscLinkedQueueNode) getNext() *mpscLinkedQueueNode {
	return this.next
}

func (this *mpscLinkedQueueNode) setNext(newNext *mpscLinkedQueueNode) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&this.next)), unsafe.Pointer(newNext))
}

func (this *mpscLinkedQueueNode) unlink() {
	this.setNext(nil)
}

func (this *mpscLinkedQueue) getAndSetTailRef(newValue *mpscLinkedQueueNode) *mpscLinkedQueueNode {
	for {
		current := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.tailRef)))
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&this.tailRef)),
			current,
			(unsafe.Pointer(newValue))) {
			return (*mpscLinkedQueueNode)(current)
		}
	}
}

func (this *mpscLinkedQueue) setHeadRef(newValue *mpscLinkedQueueNode) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&this.headRef)), unsafe.Pointer(newValue))
}

func (this *mpscLinkedQueue) peekNode() *mpscLinkedQueueNode {
	head := this.headRef
	next := head.getNext()
	if next == nil && head != this.tailRef {
		// if tail != head this is not going to change until consumer makes progress
		// we can avoid reading the head and just spin on next until it shows up
		//
		for next == nil {
			next = next.getNext()
		}
	}
	return next
}

func (this *mpscLinkedQueue) offer(e interface{}) (ok bool, err error) {

	if e == nil {
		err = errors.New("offer's value is nil")
	}

	ok = true

	newTail := new(mpscLinkedQueueNode)
	newTail.value = e
	newTail.next = nil

	oldTail := this.getAndSetTailRef(newTail)
	oldTail.setNext(newTail)
	return
}

func (this *mpscLinkedQueue) add(e interface{}) (ok bool, err error) {
	return this.offer(e)
}

func (this *mpscLinkedQueue) peek() (e interface{}) {
	next := this.peekNode()
	if next == nil {
		return nil
	}
	return next.getValue()
}

func (this *mpscLinkedQueue) poll() (e interface{}) {
	next := this.peekNode()
	if next == nil {
		return nil
	}

	// next becomes a new head.
	oldHead := this.headRef

	this.setHeadRef(next)

	// Break the linkage between the old head and the new head.
	oldHead.unlink()

	return next.getValue()
}
