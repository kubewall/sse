/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"strconv"
	"time"
)

// EventLog holds the most recent events published to a stream so that they can
// be replayed to subscribers that (re)connect.
//
// The log is bounded: Stream.run trims it via Trim after every Add, using the
// limits carried over from Server.MaxEventLogEvents and Server.MaxEventLogBytes.
// An unbounded log is a memory leak for any publisher that keeps emitting while
// no subscriber is attached -- the events stay reachable from the stream forever
// and can never be collected.
type EventLog []*Event

// Add event to eventlog
func (e *EventLog) Add(ev *Event) {
	if !ev.hasContent() {
		return
	}

	ev.ID = []byte(e.currentindex())
	ev.timestamp = time.Now()
	*e = append(*e, ev)
}

// Clear events from eventlog
func (e *EventLog) Clear() {
	*e = nil
}

// Trim drops the oldest events until the log holds at most maxEvents entries
// and at most maxBytes of event data. A non-positive limit disables that
// dimension of the bound.
//
// Dropped entries are shifted out and their slots nil'd rather than the slice
// simply being re-sliced from the front: re-slicing would leave the dropped
// *Event values reachable from the backing array, which defeats the purpose.
func (e *EventLog) Trim(maxEvents, maxBytes int) {
	log := *e
	keep := len(log)
	if keep == 0 {
		return
	}

	if maxEvents > 0 && keep > maxEvents {
		keep = maxEvents
	}

	if maxBytes > 0 {
		// Walk backwards from the newest event, keeping as many as fit.
		total := 0
		fits := 0
		for i := len(log) - 1; i >= len(log)-keep; i-- {
			total += len(log[i].Data) + len(log[i].Comment)
			if total > maxBytes && fits > 0 {
				break
			}
			fits++
		}
		keep = fits
	}

	if keep == len(log) {
		return
	}

	drop := len(log) - keep
	copy(log, log[drop:])
	for i := keep; i < len(log); i++ {
		log[i] = nil // release the dropped events for GC
	}
	*e = log[:keep]
}

// Replay events to a subscriber
func (e *EventLog) Replay(s *Subscriber) {
	for i := 0; i < len(*e); i++ {
		id, _ := strconv.Atoi(string((*e)[i].ID))
		if id >= s.eventid {
			s.connection <- (*e)[i]
		}
	}
}

// currentindex returns the ID to assign to the next event.
//
// IDs must stay monotonic across trims, otherwise a client reconnecting with
// Last-Event-ID would be matched against recycled IDs in Replay. Derive the
// next ID from the newest retained event instead of from the log length, since
// the length no longer grows once the bound is reached.
func (e *EventLog) currentindex() string {
	if n := len(*e); n > 0 {
		if id, err := strconv.Atoi(string((*e)[n-1].ID)); err == nil {
			return strconv.Itoa(id + 1)
		}
	}
	return "0"
}
