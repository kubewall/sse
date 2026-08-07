/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// autoStreamServer mirrors how kubewall configures the server: streams created
// on connect, replay enabled, event log bounded.
func autoStreamServer() *Server {
	s := New()
	s.AutoStream = true
	return s
}

// connect opens an SSE request and returns a func that disconnects it.
func connect(t *testing.T, s *Server, streamID string) func() {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(streamID, w, r)
	}))
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	waitFor(t, 2*time.Second, "subscriber to register", func() bool {
		str := s.getStream(streamID)
		return str != nil && str.getSubscriberCount() == 1
	})
	return func() {
		resp.Body.Close()
		srv.CloseClientConnections()
		srv.Close()
	}
}

// The leak this teardown exists to fix: an auto-created stream must not outlive
// its last subscriber.
func TestAutoStreamRemovedWhenLastSubscriberLeaves(t *testing.T) {
	s := autoStreamServer()
	defer s.Close()

	disconnect := connect(t, s, "pods")
	require.True(t, s.StreamExists("pods"))

	disconnect()

	waitFor(t, 3*time.Second, "stream removal", func() bool { return !s.StreamExists("pods") })
}

func TestAutoStreamSurvivesWhileAnotherSubscriberRemains(t *testing.T) {
	s := autoStreamServer()
	defer s.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP("pods", w, r)
	}))
	defer srv.Close()

	first, err := http.Get(srv.URL)
	require.NoError(t, err)
	second, err := http.Get(srv.URL)
	require.NoError(t, err)
	waitFor(t, 2*time.Second, "two subscribers", func() bool {
		str := s.getStream("pods")
		return str != nil && str.getSubscriberCount() == 2
	})

	// One tab closes: the other is still watching, so the stream must stay.
	first.Body.Close()
	waitFor(t, 2*time.Second, "one subscriber left", func() bool {
		str := s.getStream("pods")
		return str != nil && str.getSubscriberCount() == 1
	})
	assert.True(t, s.StreamExists("pods"), "stream dropped while a subscriber was still attached")

	second.Body.Close()
	srv.CloseClientConnections()
	waitFor(t, 3*time.Second, "stream removal", func() bool { return !s.StreamExists("pods") })
}

// kubewall's handler pattern: pre-register the stream so the first payload is
// not published into the void, then serve. Teardown must still reclaim it,
// otherwise pre-registering would reintroduce the leak.
func TestPreCreatedStreamStillRemovedOnDisconnect(t *testing.T) {
	s := autoStreamServer()
	defer s.Close()

	s.CreateStream("pods")
	s.Publish("pods", &Event{Data: []byte("first-paint")})
	require.True(t, s.StreamExists("pods"))

	disconnect := connect(t, s, "pods")
	disconnect()

	waitFor(t, 3*time.Second, "stream removal", func() bool { return !s.StreamExists("pods") })
}

// ...and the reason for pre-registering: without it Publish is a no-op and the
// first payload never reaches the subscriber that is about to connect.
func TestPreCreatedStreamDeliversFirstPublishToNewSubscriber(t *testing.T) {
	s := autoStreamServer()
	defer s.Close()

	s.CreateStream("pods")
	s.Publish("pods", &Event{Data: []byte("first-paint")})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP("pods", w, r)
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	var got strings.Builder
	buf := make([]byte, 256)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(got.String(), "first-paint") {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
		}
		if readErr != nil {
			break
		}
	}
	assert.Contains(t, got.String(), "first-paint",
		"a pre-registered stream must replay its first publish to the subscriber that follows")
}

// Without AutoStream the caller owns the stream lifecycle, so ServeHTTP must
// leave it alone.
func TestManualStreamNotRemovedOnDisconnect(t *testing.T) {
	s := New() // AutoStream false
	defer s.Close()

	s.CreateStream("pods")
	disconnect := connect(t, s, "pods")
	disconnect()

	time.Sleep(300 * time.Millisecond)
	assert.True(t, s.StreamExists("pods"), "a manually created stream must survive disconnects")
}

// A teardown must never remove a replacement stream registered under the same
// id by a later connection.
func TestRemoveStreamIfEmptyRespectsIdentity(t *testing.T) {
	s := autoStreamServer()
	defer s.Close()

	first := s.CreateStream("pods")
	s.removeStreamIfEmpty("pods", first)
	require.False(t, s.StreamExists("pods"))

	replacement := s.CreateStream("pods")
	require.NotSame(t, first, replacement)

	// A late teardown carrying the stale pointer must be a no-op.
	s.removeStreamIfEmpty("pods", first)
	assert.True(t, s.StreamExists("pods"), "stale teardown removed the replacement stream")
	assert.Same(t, replacement, s.getStream("pods"))
}

func TestRemoveStreamIfEmptyKeepsStreamWithSubscribers(t *testing.T) {
	s := autoStreamServer()
	defer s.Close()

	disconnect := connect(t, s, "pods")
	defer disconnect()

	s.removeStreamIfEmpty("pods", s.getStream("pods"))
	assert.True(t, s.StreamExists("pods"), "removed a stream that still had a subscriber")
}

// Prerequisite 1: close() used to send on deregister unconditionally, which
// blocks forever once run() has returned.
func TestSubscriberCloseDoesNotBlockOnClosedStream(t *testing.T) {
	str := newStream("test", 1024, true, true, 100, 0, nil, nil)
	str.run()

	sub := str.addSubscriber(0, nil)
	require.NotNil(t, sub)

	str.close()
	waitFor(t, 2*time.Second, "stream shutdown", func() bool { return isClosed(str.quit) })

	done := make(chan struct{})
	go func() { sub.close(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscriber.close() blocked on a stream whose run() had exited")
	}
}

// Prerequisite 2: addSubscriber used to send on the unbuffered register channel
// unconditionally, which blocks forever once run() has returned.
func TestAddSubscriberOnClosedStreamReturnsNil(t *testing.T) {
	str := newStream("test", 1024, true, true, 100, 0, nil, nil)
	str.run()

	// Attach a witness first. close() only makes quit *ready* -- run() may still
	// be parked in its select, where both register and quit are then ready and
	// Go picks at random. Observing the witness's connection close proves run()
	// is past removeAllSubscribers and will never read register again, which is
	// what makes the assertion below deterministic.
	witness := str.addSubscriber(0, nil)
	require.NotNil(t, witness)

	str.close()
	for range witness.connection { //nolint:revive // drain until closed
	}
	waitFor(t, 2*time.Second, "subscriber count to settle", func() bool {
		return str.getSubscriberCount() == 0
	})

	done := make(chan *Subscriber, 1)
	go func() { done <- str.addSubscriber(0, nil) }()

	select {
	case sub := <-done:
		assert.Nil(t, sub, "expected nil when registering on a closed stream")
	case <-time.After(2 * time.Second):
		t.Fatal("addSubscriber blocked on a stream whose run() had exited")
	}

	// The failed registration must not leave a phantom subscriber behind, or the
	// stream could never be considered empty again.
	assert.Equal(t, 0, str.getSubscriberCount())
}

// End to end: repeated connect/disconnect cycles must not accumulate streams or
// their run() goroutines.
func TestRepeatedConnectDisconnectLeaksNothing(t *testing.T) {
	s := autoStreamServer()
	defer s.Close()

	runtime.GC()
	baseline := runtime.NumGoroutine()

	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("stream-%d", i)
		disconnect := connect(t, s, id)
		s.Publish(id, &Event{Data: []byte("payload")})
		disconnect()
		waitFor(t, 3*time.Second, "stream "+id+" removal", func() bool { return !s.StreamExists(id) })
	}

	s.muStreams.RLock()
	remaining := len(s.streams)
	s.muStreams.RUnlock()
	assert.Equal(t, 0, remaining, "streams accumulated across connect/disconnect cycles")

	waitFor(t, 5*time.Second, "goroutines to settle", func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= baseline+2
	})
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
