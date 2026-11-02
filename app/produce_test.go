package main

import (
	"encoding/binary"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/Longseeyou/kafka-protocol-server/app/handlers"
)

func TestProduceZeroAcksPersists(t *testing.T) {
	root := t.TempDir()
	var id handlers.UUID
	metadata := handlers.ClusterMetadata{RecordBatches: []handlers.RecordBatch{{Records: []handlers.Record{
		{Value: handlers.TopicRecord{TopicName: "a", TopicUUID: id}},
		{Value: handlers.PartitionRecord{TopicUUID: id, PartitionID: 0}},
	}}}}
	batch := make([]byte, 61)
	batch[16] = 2
	binary.BigEndian.PutUint32(batch[57:61], 1)
	// A single record with null key and value, and no headers.
	batch = append(batch, 12, 0, 0, 0, 1, 1, 0)
	binary.BigEndian.PutUint32(batch[8:12], uint32(len(batch)-12))
	binary.BigEndian.PutUint32(batch[17:21], crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli)))
	payload := binary.BigEndian.AppendUint16(nil, 0)
	payload = binary.BigEndian.AppendUint16(payload, 11)
	payload = binary.BigEndian.AppendUint32(payload, 42)
	payload = append(payload, 255, 255, 0, 0)           // null client ID, header tags, null transaction
	payload = binary.BigEndian.AppendUint16(payload, 0) // acks
	payload = binary.BigEndian.AppendUint32(payload, 1000)
	payload = append(payload, 2, 2, 'a', 2) // one topic and one partition
	payload = binary.BigEndian.AppendUint32(payload, 0)
	payload = binary.AppendUvarint(payload, uint64(len(batch)+1))
	payload = append(payload, batch...)
	payload = append(payload, 0, 0, 0)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	// Any attempted response fails instead of blocking the test.
	client.Close()
	handleProduceRequest(server, payload, &metadata, handlers.NewPartitionLog(root))
	stored, err := os.ReadFile(filepath.Join(root, "a-0", "00000000000000000000.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != len(batch) {
		t.Fatalf("stored %d bytes, want %d", len(stored), len(batch))
	}
}
