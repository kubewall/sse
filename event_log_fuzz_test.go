/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Trim takes caller-supplied limits, so every degenerate combination has to be
// non-panicking. A negative or zero limit disables that dimension.
func TestEventLogTrimDegenerateLimits(t *testing.T) {
	for _, tc := range []struct {
		name                string
		maxEvents, maxBytes int
		wantLen             int
	}{
		{"both zero", 0, 0, 20},
		{"both negative", -1, -1, 20},
		{"min int", math.MinInt, math.MinInt, 20},
		{"max int", math.MaxInt, math.MaxInt, 20},
		{"events 1", 1, 0, 1},
		{"bytes 1", 0, 1, 1},
		{"both 1", 1, 1, 1},
		{"negative events, tight bytes", -5, 16, 2},
		{"tight events, negative bytes", 3, -5, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := make(EventLog, 0)
			for i := 0; i < 20; i++ {
				ev.Add(&Event{Data: []byte("12345678")}) // 8 bytes each
			}

			require.NotPanics(t, func() { ev.Trim(tc.maxEvents, tc.maxBytes) })
			assert.Equal(t, tc.wantLen, len(ev))

			// Whatever the limits, surviving entries must never be nil --
			// Replay dereferences them.
			for i, e := range ev {
				require.NotNilf(t, e, "entry %d is nil", i)
			}
		})
	}
}

func TestEventLogTrimZeroLengthPayloads(t *testing.T) {
	ev := make(EventLog, 0)

	// hasContent() passes on ID alone, so an event can legitimately enter the
	// log carrying no Data and no Comment: the byte total then never advances.
	for i := 0; i < 30; i++ {
		ev.Add(&Event{ID: []byte("seed")})
	}

	require.NotPanics(t, func() { ev.Trim(0, 8) })
	assert.Equal(t, 30, len(ev), "zero-byte events can never exceed a byte budget")

	require.NotPanics(t, func() { ev.Trim(5, 8) })
	assert.Equal(t, 5, len(ev), "the count bound still applies")
}

func TestEventLogTrimRepeatedShrinkingLimits(t *testing.T) {
	ev := make(EventLog, 0)
	for i := 0; i < 200; i++ {
		ev.Add(&Event{Data: []byte(strconv.Itoa(i))})
	}

	for _, max := range []int{150, 100, 50, 10, 3, 2, 1} {
		require.NotPanics(t, func() { ev.Trim(max, 0) })
		require.Equal(t, max, len(ev))
		// Always the newest survivors.
		assert.Equal(t, []byte("199"), ev[len(ev)-1].Data)
	}
}

func TestEventLogTrimAfterClear(t *testing.T) {
	ev := make(EventLog, 0)
	ev.Add(&Event{Data: []byte("a")})
	ev.Clear()

	require.NotPanics(t, func() { ev.Trim(10, 10) })
	assert.Equal(t, 0, len(ev))

	// The log must still be usable, and IDs restart from 0 after a Clear.
	ev.Add(&Event{Data: []byte("b")})
	require.Equal(t, 1, len(ev))
	assert.Equal(t, []byte("0"), ev[0].ID)
}

func TestEventLogCurrentIndexToleratesNonNumericID(t *testing.T) {
	ev := make(EventLog, 0)

	// Nothing stops a caller pre-seeding a non-numeric ID. currentindex must
	// fall back rather than panic; monotonicity is lost but the log survives.
	ev.Add(&Event{ID: []byte("not-a-number"), Data: []byte("x")})
	require.NotPanics(t, func() { ev.Add(&Event{Data: []byte("y")}) })
	require.NotPanics(t, func() { ev.Trim(1, 0) })

	assert.Equal(t, 1, len(ev))
}

// Randomised sequences of the whole API surface, checking the invariants Trim
// is supposed to maintain and that nothing panics.
func FuzzEventLogTrim(f *testing.F) {
	f.Add(uint8(30), uint8(7), uint16(64), uint8(3), false)
	f.Add(uint8(200), uint8(100), uint16(1024), uint8(0), true)
	f.Add(uint8(1), uint8(0), uint16(0), uint8(1), false)

	f.Fuzz(func(t *testing.T, adds, maxEvents uint8, maxBytes uint16, payload uint8, clearMidway bool) {
		ev := make(EventLog, 0)
		data := make([]byte, int(payload))

		for i := 0; i < int(adds); i++ {
			ev.Add(&Event{Data: data})
			ev.Trim(int(maxEvents), int(maxBytes))

			if clearMidway && i == int(adds)/2 {
				ev.Clear()
			}
		}

		// Invariant 1: the count bound holds.
		if maxEvents > 0 {
			require.LessOrEqual(t, len(ev), int(maxEvents))
		}

		// Invariant 2: the byte bound holds, unless a single event is larger
		// than the whole budget (the newest is always kept).
		if maxBytes > 0 && len(ev) > 1 {
			total := 0
			for _, e := range ev {
				total += len(e.Data) + len(e.Comment)
			}
			require.LessOrEqual(t, total, int(maxBytes))
		}

		// Invariant 3: no nil entries, and IDs strictly increase.
		prev := -1
		for i, e := range ev {
			require.NotNilf(t, e, "entry %d is nil", i)
			id, err := strconv.Atoi(string(e.ID))
			require.NoErrorf(t, err, "entry %d has a non-numeric ID", i)
			require.Greaterf(t, id, prev, "entry %d ID went backwards", i)
			prev = id
		}

		// Invariant 4: nothing past len still references a dropped event.
		full := ev[:cap(ev)]
		for i := len(ev); i < len(full); i++ {
			require.Nilf(t, full[i], "slot %d past len retains a dropped event", i)
		}

		// Replay over a trimmed log must not panic. Buffer generously so the
		// send cannot block.
		sub := &Subscriber{connection: make(chan *Event, int(adds)+1)}
		require.NotPanics(t, func() { ev.Replay(sub) })
	})
}
