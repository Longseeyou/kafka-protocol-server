package handlers

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var batchCRC = crc32.MakeTable(crc32.Castagnoli)

// PartitionLog serializes offset allocation and appends across connections.
// A single lock keeps this small broker's storage implementation simple.
type PartitionLog struct {
	root string
	mu   sync.Mutex
}

func NewPartitionLog(root string) *PartitionLog { return &PartitionLog{root: root} }

// walkBatches checks framing and CRC without interpreting application values.
func walkBatches(data []byte, visit func([]byte) error) error {
	for len(data) > 0 {
		if len(data) < 61 {
			return fmt.Errorf("truncated record batch")
		}
		n := int64(int32(binary.BigEndian.Uint32(data[8:12]))) + 12
		if n < 61 || n > int64(len(data)) {
			return fmt.Errorf("invalid batch length")
		}
		batch := data[:int(n)]
		if batch[16] != 2 {
			return fmt.Errorf("unsupported record batch magic")
		}
		if binary.BigEndian.Uint32(batch[17:21]) != crc32.Checksum(batch[21:], batchCRC) {
			return fmt.Errorf("record batch CRC mismatch")
		}
		if int32(binary.BigEndian.Uint32(batch[23:27])) < 0 || int32(binary.BigEndian.Uint32(batch[57:61])) <= 0 {
			return fmt.Errorf("invalid record count or offset delta")
		}
		if err := visit(batch); err != nil {
			return err
		}
		data = data[int(n):]
	}
	return nil
}

// Append preserves encoded records and changes only the CRC-exempt base offset.
// Existing batches are scanned so offsets also survive process restarts.
func (s *PartitionLog) Append(topic string, partition int32, records []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if topic == "" || topic == "." || topic == ".." || strings.ContainsAny(topic, `/\:`) || partition < 0 {
		return -1, fmt.Errorf("invalid topic or partition")
	}
	if len(records) == 0 {
		return -1, fmt.Errorf("empty record set")
	}
	if err := walkBatches(records, func([]byte) error { return nil }); err != nil {
		return -1, err
	}
	path := filepath.Join(s.root, fmt.Sprintf("%s-%d", topic, partition), "00000000000000000000.log")
	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return -1, err
	}
	var next int64
	if err := walkBatches(existing, func(batch []byte) error {
		base := int64(binary.BigEndian.Uint64(batch[:8]))
		delta := int64(binary.BigEndian.Uint32(batch[23:27]))
		if base < next || base > math.MaxInt64-delta-1 {
			return fmt.Errorf("invalid stored offsets")
		}
		next = base + delta + 1
		return nil
	}); err != nil {
		return -1, err
	}
	first := next
	data := append([]byte(nil), records...)
	if err := walkBatches(data, func(batch []byte) error {
		delta := int64(binary.BigEndian.Uint32(batch[23:27]))
		if next > math.MaxInt64-delta-1 {
			return fmt.Errorf("offset overflow")
		}
		binary.BigEndian.PutUint64(batch[:8], uint64(next))
		next += delta + 1
		return nil
	}); err != nil {
		return -1, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return -1, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return -1, err
	}
	n, writeErr := f.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = fmt.Errorf("short log write")
	}
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if writeErr != nil {
		rollbackErr := f.Truncate(int64(len(existing)))
		closeErr := f.Close()
		return -1, fmt.Errorf("append: %w (rollback: %v, close: %v)", writeErr, rollbackErr, closeErr)
	}
	if err := f.Close(); err != nil {
		return -1, err
	}
	return first, nil
}

// Produce validates metadata before writing and reports each partition separately.
func (s *PartitionLog) Produce(request ProduceRequest, metadata TopicMetadata) ProduceResponse {
	response := BuildProduceResponse(request, metadata)
	for i, topic := range request.Topics {
		for j, partition := range topic.Partitions {
			result := &response.Topics[i].Partitions[j]
			if result.ErrorCode != 0 {
				continue
			}
			base, err := s.Append(topic.Name, partition.Index, partition.Records)
			if err != nil {
				result.ErrorCode = -1 // UNKNOWN_SERVER_ERROR
				result.BaseOffset = -1
				result.LogStartOffset = -1
				continue
			}
			result.BaseOffset = base
		}
	}
	return response
}
