/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventLogTrimKeepsNewestByCount(t *testing.T) {
	ev := make(EventLog, 0)

	for i := 0; i < 250; i++ {
		ev.Add(&Event{Data: []byte(strconv.Itoa(i))})
		ev.Trim(100, 0)
	}

	require.Equal(t, 100, len(ev))
	// The newest 100 are the ones retained: 150..249.
	assert.Equal(t, []byte("150"), ev[0].Data)
	assert.Equal(t, []byte("249"), ev[len(ev)-1].Data)
}

func TestEventLogTrimByBytes(t *testing.T) {
	ev := make(EventLog, 0)
	payload := make([]byte, 300*1024) // 300 KiB, like a resource-list snapshot

	for i := 0; i < 20; i++ {
		ev.Add(&Event{Data: payload})
		ev.Trim(100, 1<<20) // count bound is slack; the byte bound must bind
	}

	// 1 MiB / 300 KiB => 3 events fit.
	assert.Equal(t, 3, len(ev))

	total := 0
	for _, e := range ev {
		total += len(e.Data)
	}
	assert.LessOrEqual(t, total, 1<<20)
}

func TestEventLogTrimByBytesWithNoCountLimit(t *testing.T) {
	ev := make(EventLog, 0)

	for i := 0; i < 50; i++ {
		ev.Add(&Event{Data: make([]byte, 1024)})
		ev.Trim(0, 4096) // count disabled, bytes only
	}

	assert.Equal(t, 4, len(ev))
}

func TestEventLogTrimCountsCommentBytes(t *testing.T) {
	ev := make(EventLog, 0)

	// hasContent() does not look at Comment, so an event needs Data to enter
	// the log at all -- but once in, its Comment still occupies memory and has
	// to count against the byte budget. 512+512 per event => 4 fit in 4 KiB.
	for i := 0; i < 50; i++ {
		ev.Add(&Event{Data: make([]byte, 512), Comment: make([]byte, 512)})
		ev.Trim(0, 4096)
	}

	assert.Equal(t, 4, len(ev))
}

func TestEventLogAddIgnoresCommentOnlyEvents(t *testing.T) {
	ev := make(EventLog, 0)

	// Pre-existing upstream behaviour, pinned here because Trim's byte
	// accounting is written on the assumption that Add is the only way in.
	ev.Add(&Event{Comment: []byte("keep-alive")})

	assert.Equal(t, 0, len(ev))
}

func TestEventLogTrimEmpty(t *testing.T) {
	ev := make(EventLog, 0)

	ev.Trim(10, 1024) // must not panic on an empty log
	assert.Equal(t, 0, len(ev))
}

func TestEventLogTrimAlwaysKeepsNewest(t *testing.T) {
	ev := make(EventLog, 0)

	// A single event larger than the whole byte budget must still be retained,
	// otherwise a reconnecting subscriber would get nothing at all.
	ev.Add(&Event{Data: make([]byte, 4<<20)})
	ev.Trim(100, 1<<20)

	assert.Equal(t, 1, len(ev))
}

func TestEventLogTrimReleasesDroppedEvents(t *testing.T) {
	ev := make(EventLog, 0)

	for i := 0; i < 500; i++ {
		ev.Add(&Event{Data: []byte(strconv.Itoa(i))})
		ev.Trim(10, 0)
	}

	require.Equal(t, 10, len(ev))

	// Re-slicing from the front would leave dropped events reachable from the
	// backing array. Every slot past len must be nil.
	full := ev[:cap(ev)]
	for i := len(ev); i < len(full); i++ {
		assert.Nilf(t, full[i], "slot %d past len still references a dropped event", i)
	}
}

func TestEventLogIDsStayMonotonicAcrossTrims(t *testing.T) {
	ev := make(EventLog, 0)

	for i := 0; i < 250; i++ {
		ev.Add(&Event{Data: []byte("x")})
		ev.Trim(10, 0)
	}

	// IDs must not be recycled once the log stops growing, otherwise a client
	// reconnecting with Last-Event-ID is matched against the wrong events.
	last, err := strconv.Atoi(string(ev[len(ev)-1].ID))
	require.NoError(t, err)
	assert.Equal(t, 249, last)

	prev := -1
	for _, e := range ev {
		id, err := strconv.Atoi(string(e.ID))
		require.NoError(t, err)
		assert.Greater(t, id, prev)
		prev = id
	}
}

func TestEventLogTrimUnboundedWhenLimitsDisabled(t *testing.T) {
	ev := make(EventLog, 0)

	for i := 0; i < 200; i++ {
		ev.Add(&Event{Data: []byte("x")})
		ev.Trim(0, 0)
	}

	assert.Equal(t, 200, len(ev))
}

// The leak this bound exists to fix: a stream that is published to while no
// subscriber is attached must not accumulate events without limit. Asserted
// behaviourally (via what a late subscriber is replayed) so the Eventlog is
// only ever touched by the stream's own goroutine.
func TestStreamBoundsEventlogWithNoSubscribers(t *testing.T) {
	s := newStream("test", 1024, true, false, 10, 0, nil, nil)
	s.run()
	defer s.close()

	for i := 0; i < 500; i++ {
		s.event <- &Event{Data: []byte(fmt.Sprintf("event-%d", i))}
	}
	time.Sleep(time.Millisecond * 200)

	sub := s.addSubscriber(0, nil)

	replayed := make([][]byte, 0, 16)
	for {
		msg, err := wait(sub.connection, time.Millisecond*200)
		if err != nil {
			break
		}
		replayed = append(replayed, msg)
	}

	require.Equal(t, 10, len(replayed), "a late subscriber should only be replayed the bounded tail")
	assert.Equal(t, []byte("event-490"), replayed[0])
	assert.Equal(t, []byte("event-499"), replayed[len(replayed)-1])
}

// The Server defaults must reach the streams it creates, otherwise the bound is
// silently inert.
func TestServerPropagatesEventLogBoundsToStreams(t *testing.T) {
	s := New()
	defer s.Close()

	assert.Equal(t, DefaultMaxEventLogEvents, s.MaxEventLogEvents)
	assert.Equal(t, DefaultMaxEventLogBytes, s.MaxEventLogBytes)

	str := s.CreateStream("defaults")
	assert.Equal(t, DefaultMaxEventLogEvents, str.maxEventLogEvents)
	assert.Equal(t, DefaultMaxEventLogBytes, str.maxEventLogBytes)

	s.MaxEventLogEvents = 7
	s.MaxEventLogBytes = 128
	str = s.CreateStream("overridden")
	assert.Equal(t, 7, str.maxEventLogEvents)
	assert.Equal(t, 128, str.maxEventLogBytes)
}

func TestServerNewWithCallbackSetsEventLogBounds(t *testing.T) {
	s := NewWithCallback(nil, nil)
	defer s.Close()

	assert.Equal(t, DefaultMaxEventLogEvents, s.MaxEventLogEvents)
	assert.Equal(t, DefaultMaxEventLogBytes, s.MaxEventLogBytes)
}

// A closed stream must drop its retained events, so that a lingering reference
// to the *Stream does not keep the payloads alive.
func TestStreamClearsEventlogOnShutdown(t *testing.T) {
	s := newStream("test", 1024, true, false, 100, 0, nil, nil)
	s.run()

	sub := s.addSubscriber(0, nil)
	s.event <- &Event{Data: []byte("retained")}
	_, err := wait(sub.connection, time.Second)
	require.NoError(t, err)

	s.close()

	// Draining until the connection is closed synchronises with run()'s
	// shutdown path, which clears the Eventlog before closing subscribers.
	for range sub.connection {
	}

	assert.Equal(t, 0, len(s.Eventlog))
}
