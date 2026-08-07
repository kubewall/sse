/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import "net/url"

// Subscriber ...
type Subscriber struct {
	quit chan *Subscriber
	// streamQuit is closed when the owning stream shuts down. It lets close()
	// give up instead of blocking forever on a run() loop that has returned.
	streamQuit <-chan struct{}
	connection chan *Event
	removed    chan struct{}
	eventid    int
	URL        *url.URL
}

// Close will let the stream know that the clients connection has terminated
func (s *Subscriber) close() {
	select {
	case s.quit <- s:
		if s.removed != nil {
			<-s.removed
		}
	case <-s.streamQuit:
		// The stream is gone: run() already removed every subscriber on its way
		// out, so there is nothing to deregister and nothing to wait for.
	}
}
