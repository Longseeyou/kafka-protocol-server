package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"time"
)

func TestFetchNoTopics(t *testing.T) {
	// Fetch v16 with empty topics and forgotten topics, no session, empty rack ID.
	request, err := hex.DecodeString("000000300001001037a6989a000c6b61666b612d74657374657200000001f4000000010320000000000000000000000001010100")
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnection(server, nil, nil)
	}()
	t.Cleanup(func() {
		client.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("connection handler did not exit")
		}
	})
	if err := client.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, correlationID := range []uint32{0x37a6989a, 0x12345678} {
		binary.BigEndian.PutUint32(request[8:12], correlationID)
		if _, err := client.Write(request); err != nil {
			t.Fatal(err)
		}
		response := make([]byte, 21)
		if _, err := io.ReadFull(client, response); err != nil {
			t.Fatal(err)
		}
		if size := binary.BigEndian.Uint32(response[:4]); size != 17 {
			t.Fatalf("frame size = %d, want 17", size)
		}
		if got := binary.BigEndian.Uint32(response[4:8]); got != correlationID {
			t.Fatalf("correlation ID = %x, want %x", got, correlationID)
		}
		// Header tags, throttle, error, session, empty compact array, body tags.
		want := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0}
		if !bytes.Equal(response[8:], want) {
			t.Fatalf("response fields = %x, want %x", response[8:], want)
		}
	}
}
