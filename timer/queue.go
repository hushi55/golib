package timer

import (
	"sync/atomic"
	"unsafe"
	"errors"
)

type Queue interface {
	
	poll() (e interface{});
	peek() (e interface{});
	
	offer(e interface{}) (ok bool, err error);
	add(e interface{}) (ok bool, err error);
	
}

func NewQueue() Queue {
	return new(mpscLinkedQueue)
}

type mpscLinkedQueue struct {
	p00, p01, p02, p03, p04, p05, p06, p07 int64

	headRef *mpscLinkedQueueNode

	p10, p11, p12, p13, p14, p15, p16, p17 int64

	tailRef *mpscLinkedQueueNode

	p20, p21, p22, p23, p24, p25, p26, p27 int64
}

type mpscLinkedQueueNode struct {
	p00, p01, p02, p03, p04, p05, p06, p07 int64

	next unsafe.Pointer
//	next *mpscLinkedQueueNode

	p30, p31, p32, p33, p34, p35, p36, p37 int64

	value interface{}
}

func (this *mpscLinkedQueueNode) getValue() (v interface{}) {
	return this.value
}

func (this *mpscLinkedQueueNode) getNext() *mpscLinkedQueueNode {
	return (*mpscLinkedQueueNode)(this.next)
}

func (this *mpscLinkedQueueNode) setNext(newNext *mpscLinkedQueueNode) {
	atomic.StorePointer(&this.next, unsafe.Pointer(newNext))
}

func (this *mpscLinkedQueueNode) unlink() {
	this.setNext(nil)
}

func (this *mpscLinkedQueue) getAndSetTailRef(newValue *mpscLinkedQueueNode) *mpscLinkedQueueNode {
	for {

		//		p : = unsafe.Pointer(this.tailRef)
		current := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(this.tailRef)))
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(this.tailRef)),
			current,
			(unsafe.Pointer(newValue))) {
			return (*mpscLinkedQueueNode)(current)
		}
	}
}

//func (this *mpscLinkedQueue) getAndSetHeadRef(newValue unsafe.Pointer) unsafe.Pointer {
//	for {
//		current := atomic.LoadPointer(&this.headRef)
//		if atomic.CompareAndSwapPointer(&this.headRef, current, newValue) {
//			return current
//		}
//	}
//}

func (this *mpscLinkedQueue) SetHeadRef(newValue *mpscLinkedQueueNode) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(this.headRef)), unsafe.Pointer(newValue))
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

func (this *mpscLinkedQueue) offer(e interface{}) (ok bool, err error){
	
	if e == nil {
		err = errors.New("offer's value is nil");
	}
	
	ok = true
	
	newTail := new(mpscLinkedQueueNode)
	newTail.value = e
	newTail.next = nil

	oldTail := this.getAndSetTailRef(newTail)
	oldTail.setNext(newTail)
	return
}

func (this *mpscLinkedQueue) add(e interface{}) (ok bool, err error){
	return this.offer(e);
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
	// Similar to 'headRef.node = next', but slightly faster (storestore vs loadstore)
	// See: http://robsjava.blogspot.com/2013/06/a-faster-volatile.html
	// See: http://psy-lob-saw.blogspot.com/2012/12/atomiclazyset-is-performance-win-for.html
	this.SetHeadRef(next)

	// Break the linkage between the old head and the new head.
	oldHead.unlink()

	return next.getValue()
}
