/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"net/url"
	"sync"
	"sync/atomic"
)

// Stream ...
type Stream struct {
	ID              string
	event           chan *Event
	quit            chan struct{}
	quitOnce        sync.Once
	register        chan *Subscriber
	deregister      chan *Subscriber
	subscribers     []*Subscriber
	Eventlog        EventLog
	subscriberCount int32
	// Enables replaying of eventlog to newly added subscribers
	AutoReplay   bool
	isAutoStream bool
	// Upper bounds on the retained Eventlog, copied from the Server.
	maxEventLogEvents int
	maxEventLogBytes  int

	// Specifies the function to run when client subscribe or un-subscribe
	OnSubscribe   func(streamID string, sub *Subscriber)
	OnUnsubscribe func(streamID string, sub *Subscriber)
}

// newStream returns a new stream
func newStream(id string, buffSize int, replay, isAutoStream bool, maxEventLogEvents, maxEventLogBytes int, onSubscribe, onUnsubscribe func(string, *Subscriber)) *Stream {
	return &Stream{
		ID:                id,
		AutoReplay:        replay,
		subscribers:       make([]*Subscriber, 0),
		isAutoStream:      isAutoStream,
		maxEventLogEvents: maxEventLogEvents,
		maxEventLogBytes:  maxEventLogBytes,
		register:          make(chan *Subscriber),
		deregister:        make(chan *Subscriber),
		event:             make(chan *Event, buffSize),
		quit:              make(chan struct{}),
		Eventlog:          make(EventLog, 0),
		OnSubscribe:       onSubscribe,
		OnUnsubscribe:     onUnsubscribe,
	}
}

func (str *Stream) run() {
	go func(str *Stream) {
		for {
			select {
			// Add new subscriber
			case subscriber := <-str.register:
				str.subscribers = append(str.subscribers, subscriber)
				if str.AutoReplay {
					str.Eventlog.Replay(subscriber)
				}

			// Remove closed subscriber
			case subscriber := <-str.deregister:
				i := str.getSubIndex(subscriber)
				if i != -1 {
					str.removeSubscriber(i)
				}

				if str.OnUnsubscribe != nil {
					go str.OnUnsubscribe(str.ID, subscriber)
				}

			// Publish event to subscribers
			case event := <-str.event:
				if str.AutoReplay {
					str.Eventlog.Add(event)
					// Bound the log here, inside run(), so Eventlog is only
					// ever touched by this goroutine.
					str.Eventlog.Trim(str.maxEventLogEvents, str.maxEventLogBytes)
				}
				for i := range str.subscribers {
					str.subscribers[i].connection <- event
				}

			// Shutdown if the server closes
			case <-str.quit:
				// Drop the retained events so they are collectable even if
				// something still holds a reference to this Stream. Done
				// before removeAllSubscribers so that observing a subscriber's
				// closed connection also guarantees visibility of this write.
				str.Eventlog.Clear()
				// remove connections
				str.removeAllSubscribers()
				return
			}
		}
	}(str)
}

func (str *Stream) close() {
	str.quitOnce.Do(func() {
		close(str.quit)
	})
}

func (str *Stream) getSubIndex(sub *Subscriber) int {
	for i := range str.subscribers {
		if str.subscribers[i] == sub {
			return i
		}
	}
	return -1
}

// addSubscriber will create a new subscriber on a stream.
//
// It returns nil if the stream was shut down before the subscriber could be
// registered, which an AutoStream server can do the moment its last subscriber
// leaves. Callers must handle nil (ServeHTTP retries against a fresh stream)
// rather than assume success.
func (str *Stream) addSubscriber(eventid int, url *url.URL) *Subscriber {
	// Counted before registering, so a teardown racing with this connect sees a
	// non-zero count and leaves the stream alone.
	atomic.AddInt32(&str.subscriberCount, 1)
	sub := &Subscriber{
		eventid:    eventid,
		quit:       str.deregister,
		streamQuit: str.quit,
		connection: make(chan *Event, 64),
		URL:        url,
	}

	if str.isAutoStream {
		sub.removed = make(chan struct{}, 1)
	}

	select {
	case str.register <- sub:
	case <-str.quit:
		// run() has returned; nothing will ever read str.register. Undo the
		// count so the (already dead) stream is not held up by a phantom
		// subscriber.
		atomic.AddInt32(&str.subscriberCount, -1)
		return nil
	}

	if str.OnSubscribe != nil {
		go str.OnSubscribe(str.ID, sub)
	}

	return sub
}

func (str *Stream) removeSubscriber(i int) {
	atomic.AddInt32(&str.subscriberCount, -1)
	close(str.subscribers[i].connection)
	if str.subscribers[i].removed != nil {
		str.subscribers[i].removed <- struct{}{}
		close(str.subscribers[i].removed)
	}
	str.subscribers = append(str.subscribers[:i], str.subscribers[i+1:]...)
}

func (str *Stream) removeAllSubscribers() {
	n := len(str.subscribers)
	for i := 0; i < n; i++ {
		close(str.subscribers[i].connection)
		if str.subscribers[i].removed != nil {
			str.subscribers[i].removed <- struct{}{}
			close(str.subscribers[i].removed)
		}
	}
	// Subtract rather than storing zero: a connect racing with this shutdown may
	// already have incremented the count, and clobbering it would let that
	// connect's own decrement drive the counter negative.
	atomic.AddInt32(&str.subscriberCount, -int32(n))
	str.subscribers = str.subscribers[:0]
}

func (str *Stream) getSubscriberCount() int {
	return int(atomic.LoadInt32(&str.subscriberCount))
}
