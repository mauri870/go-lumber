// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package v2

import (
	"net"
	"sync"
	"testing"
	"time"
)

// mockConn is a mock implementation of net.Conn for testing
type mockConn struct {
	closed bool
	mu     sync.Mutex
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	// Mock implementation - just block or return EOF if closed
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, net.ErrClosed
	}
	// Block to simulate a real connection
	time.Sleep(time.Millisecond * 100)
	return 0, net.ErrClosed
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, net.ErrClosed
	}
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// TestAsyncClientSendAfterClose tests that calling Send after Close doesn't panic
func TestAsyncClientSendAfterClose(t *testing.T) {
	// Create a mock connection
	conn := &mockConn{}
	
	// Create async client with the mock connection
	client, err := NewAsyncClientWithConn(conn, 1) // Small inflight to increase chance of race
	if err != nil {
		t.Fatalf("Failed to create async client: %v", err)
	}
	
	// Close the client
	if err := client.Close(); err != nil {
		t.Logf("Error closing client: %v", err) // Log but don't fail, some errors expected
	}
	
	// Try to send after close multiple times - this should not panic
	for i := 0; i < 10; i++ {
		callback := func(seq uint32, err error) {
			// Callback might not be called due to closed client
		}
		
		// This call should not panic even though the client is closed
		client.Send(callback, []interface{}{map[string]interface{}{"test": i}})
	}
}

// TestAsyncClientSendOnClosedChannelRace attempts to reproduce the "send on closed channel" panic
// by creating a race condition between Send and Close
func TestAsyncClientSendOnClosedChannelRace(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		func() {
			// Create a mock connection
			conn := &mockConn{}
			
			// Create async client with small inflight buffer to increase race chances
			client, err := NewAsyncClientWithConn(conn, 1)
			if err != nil {
				t.Fatalf("Failed to create async client: %v", err)
			}
			
			var wg sync.WaitGroup
			
			// Start multiple goroutines that continuously send
			for g := 0; g < 5; g++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()
					for i := 0; i < 10; i++ {
						callback := func(seq uint32, err error) {}
						client.Send(callback, []interface{}{map[string]interface{}{"goroutine": goroutineID, "data": i}})
						// No sleep to maximize race opportunity
					}
				}(g)
			}
			
			// Start goroutine that closes the client
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Microsecond * 5) // Very small delay
				client.Close()
			}()
			
			wg.Wait()
		}()
	}
}

// TestAsyncClientDirectChannelAccess attempts to reproduce the panic by directly
// manipulating the internal state in a way that might cause the race condition
func TestAsyncClientDirectChannelAccess(t *testing.T) {
	conn := &mockConn{}
	
	client, err := NewAsyncClientWithConn(conn, 10)
	if err != nil {
		t.Fatalf("Failed to create async client: %v", err)
	}
	
	// Use reflection or other means to access internal channel if needed
	// For now, just test the external API aggressively
	
	var wg sync.WaitGroup
	const numGoroutines = 20
	const sendsPerGoroutine = 100
	
	// Start many goroutines sending
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < sendsPerGoroutine; j++ {
				callback := func(seq uint32, err error) {
					// Do nothing, we're just trying to trigger the race
				}
				// Continuously send to try to hit race condition
				client.Send(callback, []interface{}{map[string]interface{}{"sender": id, "seq": j}})
			}
		}(i)
	}
	
	// Close the client while sends are happening
	go func() {
		time.Sleep(time.Millisecond * 10) // Let some sends start
		client.Close()
	}()
	
	wg.Wait()
}

// TestAsyncClientConcurrentSendAndClose tests concurrent Send and Close operations
func TestAsyncClientConcurrentSendAndClose(t *testing.T) {
	conn := &mockConn{}
	
	client, err := NewAsyncClientWithConn(conn, 10)
	if err != nil {
		t.Fatalf("Failed to create async client: %v", err)
	}
	
	var wg sync.WaitGroup
	numGoroutines := 10
	
	// Start multiple goroutines sending data
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				callback := func(seq uint32, err error) {
					// Callback might be called with error due to close
				}
				// This should not panic even if called concurrently with Close
				client.Send(callback, []interface{}{map[string]interface{}{"id": id, "msg": j}})
				time.Sleep(time.Millisecond * 10)
			}
		}(i)
	}
	
	// Close the client while sends are happening
	time.Sleep(time.Millisecond * 50)
	client.Close()
	
	// Wait for all goroutines to complete
	wg.Wait()
}

// TestAsyncClientMultipleClose tests that multiple Close calls don't cause issues
func TestAsyncClientMultipleClose(t *testing.T) {
	conn := &mockConn{}
	
	client, err := NewAsyncClientWithConn(conn, 5)
	if err != nil {
		t.Fatalf("Failed to create async client: %v", err)
	}
	
	// Close multiple times - should not panic
	client.Close()
	client.Close()
	client.Close()
	
	// Try to send after multiple closes - should not panic
	callback := func(seq uint32, err error) {}
	client.Send(callback, []interface{}{"test"})
}

// TestOriginalSendOnClosedChannelPanic attempts to reproduce the specific panic
// mentioned in the original issue: "panic: send on closed channel" at line 129 (trySend call)
func TestOriginalSendOnClosedChannelPanic(t *testing.T) {
	// This test attempts to reproduce the exact scenario from the original panic
	for attempt := 0; attempt < 50; attempt++ {
		func() {
			conn := &mockConn{}
			
			client, err := NewAsyncClientWithConn(conn, 1)
			if err != nil {
				t.Fatalf("Failed to create async client: %v", err)
			}
			
			// Channel to signal when we should close the client
			closeSignal := make(chan struct{})
			
			var wg sync.WaitGroup
			
			// Goroutine that will send data continuously
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					select {
					case <-closeSignal:
						return
					default:
					}
					
					callback := func(seq uint32, err error) {
						// This callback might be called after close
					}
					
					// This call should not panic even if client is closed concurrently
					client.Send(callback, []interface{}{map[string]interface{}{
						"message": "test data",
						"id":      i,
					}})
				}
			}()
			
			// Goroutine that closes the client after a very short delay
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Microsecond * 100) // Small delay to allow some sends
				close(closeSignal)
				client.Close()
			}()
			
			wg.Wait()
		}()
	}
}