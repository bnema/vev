package main

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestUDPNetemDropsWhenBoundedSchedulerIsFull(t *testing.T) {
	r := &udpNetemRelay{
		done:  make(chan struct{}),
		slots: make(chan struct{}, udpNetemQueueCapacity),
		queue: make(chan scheduledPacket, udpNetemQueueCapacity),
	}
	packet := scheduledPacket{packet: []byte("bounded"), to: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}}
	for range udpNetemQueueCapacity {
		if !r.enqueue(packet) {
			t.Fatal("enqueue() dropped packet before scheduler capacity was full")
		}
	}
	if r.enqueue(packet) {
		t.Fatal("enqueue() accepted packet beyond scheduler capacity")
	}
	if got := r.queueOverflowDrops.Load(); got != 1 {
		t.Fatalf("queue overflow drops=%d, want 1", got)
	}
}

func TestUDPNetemExecutesRTTAndLossFixtures(t *testing.T) {
	fixtures := []struct {
		name string
		rtt  time.Duration
		loss int
		sent int
		want int
	}{
		{"25ms RTT", 25 * time.Millisecond, 0, 1, 1},
		{"100ms RTT", 100 * time.Millisecond, 0, 1, 1},
		{"zero loss", 0, 0, 10, 10},
		{"one percent loss", 0, 1, 100, 99},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			target, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			path := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(path, []byte("VEV-UDP "+strconv.Itoa(target.LocalAddr().(*net.UDPAddr).Port)+" key\\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			netem, err := newUDPNetem(udpNetemConfig{RTT: tc.rtt, LossPercent: tc.loss, TargetPath: path})
			if err != nil {
				t.Fatal(err)
			}
			defer netem.Close()
			client, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			started := time.Now()
			for i := 0; i < tc.sent; i++ {
				if _, err := client.WriteTo([]byte{byte(i)}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: netem.Port()}); err != nil {
					t.Fatal(err)
				}
			}
			if err := target.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			got := 0
			buf := make([]byte, 32)
			var relayAddr net.Addr
			for got < tc.want {
				if _, relayAddr, err = target.ReadFrom(buf); err != nil {
					t.Fatalf("received %d packets, want %d: %v", got, tc.want, err)
				}
				got++
			}
			if tc.rtt > 0 {
				if elapsed := time.Since(started); elapsed < tc.rtt/2 {
					t.Fatalf("client-to-target netem delay=%s, want at least %s", elapsed, tc.rtt/2)
				}
			}
			if tc.loss == 0 {
				// The relay learns the client from the first carriage packet. Replying
				// through that same relay address verifies the target-to-client half
				// of the configured RTT as well.
				started = time.Now()
				if _, err := target.WriteTo([]byte("reply"), relayAddr); err != nil {
					t.Fatal(err)
				}
				if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, _, err := client.ReadFrom(buf); err != nil {
					t.Fatalf("relay reply: %v", err)
				}
				if tc.rtt > 0 {
					if elapsed := time.Since(started); elapsed < tc.rtt/2 {
						t.Fatalf("target-to-client netem delay=%s, want at least %s", elapsed, tc.rtt/2)
					}
				}
			}
			// Keep the deadline short: extra carriage would prove the exact 1%%
			// loss cycle was not enforced.
			if err := target.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if _, _, err := target.ReadFrom(buf); tc.want != tc.sent && err == nil {
				t.Fatalf("received more than %d packets with %d%% loss", tc.want, tc.loss)
			}
		})
	}
}
