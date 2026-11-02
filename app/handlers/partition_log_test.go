package handlers

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func testBatch(values ...string) []byte {
	b := make([]byte, 61)
	b[16] = 2
	binary.BigEndian.PutUint32(b[23:27], uint32(len(values)-1))
	binary.BigEndian.PutUint32(b[57:61], uint32(len(values)))
	for i, v := range values {
		r := []byte{0}
		r = binary.AppendVarint(r, 0)
		r = binary.AppendVarint(r, int64(i))
		r = binary.AppendVarint(r, -1)
		r = binary.AppendVarint(r, int64(len(v)))
		r = append(r, v...)
		r = binary.AppendVarint(r, 0)
		b = binary.AppendVarint(b, int64(len(r)))
		b = append(b, r...)
	}
	binary.BigEndian.PutUint32(b[8:12], uint32(len(b)-12))
	binary.BigEndian.PutUint32(b[17:21], crc32.Checksum(b[21:], batchCRC))
	return b
}

func TestAppendMultipleRecordsBatchesAndRestart(t *testing.T) {
	root := t.TempDir()
	s := NewPartitionLog(root)
	first := testBatch("one", "two", "three")
	second := testBatch("four", "five")
	input := append(append([]byte(nil), first...), second...)
	if base, err := s.Append("fruit", 0, input); err != nil || base != 0 {
		t.Fatalf("%d %v", base, err)
	}
	s = NewPartitionLog(root)
	if base, err := s.Append("fruit", 0, testBatch("six")); err != nil || base != 5 {
		t.Fatalf("%d %v", base, err)
	}
	if base, err := s.Append("fruit", 1, testBatch("independent")); err != nil || base != 0 {
		t.Fatalf("%d %v", base, err)
	}
	data, err := os.ReadFile(filepath.Join(root, "fruit-0", "00000000000000000000.log"))
	if err != nil {
		t.Fatal(err)
	}
	var offsets []uint64
	if err := walkBatches(data, func(b []byte) error { offsets = append(offsets, binary.BigEndian.Uint64(b)); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 3 || offsets[0] != 0 || offsets[1] != 3 || offsets[2] != 5 {
		t.Fatal(offsets)
	}
	if !bytes.Equal(data[61:len(first)], first[61:]) {
		t.Fatal("records changed")
	}
	if binary.BigEndian.Uint64(input[len(first):]) != 0 {
		t.Fatal("input mutated")
	}
}

func TestRejectInvalidBatchesWithoutAppending(t *testing.T) {
	root := t.TempDir()
	s := NewPartitionLog(root)
	good := testBatch("ok")
	if _, err := s.Append("a", 0, good); err != nil {
		t.Fatal(err)
	}
	bad := testBatch("bad")
	bad[len(bad)-1] ^= 1
	for _, data := range [][]byte{nil, good[:60], append(append([]byte(nil), good...), bad...)} {
		if _, err := s.Append("a", 0, data); err == nil {
			t.Fatal("accepted invalid batch")
		}
	}
	if base, err := s.Append("a", 0, good); err != nil || base != 1 {
		t.Fatalf("%d %v", base, err)
	}
}

func TestConcurrentOffsets(t *testing.T) {
	s := NewPartitionLog(t.TempDir())
	var wg sync.WaitGroup
	offsets := make(chan int64, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			base, err := s.Append("a", 0, testBatch("x", "y"))
			if err != nil {
				t.Error(err)
				return
			}
			offsets <- base
		}()
	}
	wg.Wait()
	close(offsets)
	seen := map[int64]bool{}
	for base := range offsets {
		if seen[base] || base%2 != 0 {
			t.Errorf("bad offset %d", base)
		}
		seen[base] = true
	}
	if len(seen) != 10 {
		t.Fatal(seen)
	}
}

func TestProducePartitionResults(t *testing.T) {
	var id UUID
	metadata := TopicMetadata{Topics: map[string]Topic{"a": {TopicID: id}}, Partitions: map[UUID][]Partition{id: {{PartitionIndex: 0}}}}
	s := NewPartitionLog(t.TempDir())
	request := ProduceRequest{Topics: []ProduceTopic{{Name: "a", Partitions: []ProducePartition{
		{Index: 0, Records: testBatch("one", "two")},
		{Index: 1, Records: testBatch("invalid partition")},
	}}, {Name: "missing", Partitions: []ProducePartition{{Index: 0, Records: testBatch("invalid topic")}}}}}
	response := s.Produce(request, metadata)
	if response.Topics[0].Partitions[0].ErrorCode != 0 || response.Topics[0].Partitions[1].ErrorCode != 3 || response.Topics[1].Partitions[0].ErrorCode != 3 {
		t.Fatalf("unexpected response: %+v", response)
	}
	response = s.Produce(request, metadata)
	if response.Topics[0].Partitions[0].BaseOffset != 2 {
		t.Fatal("offset did not advance")
	}
	request.Topics[0].Partitions[0].Records = []byte{1}
	response = s.Produce(request, metadata)
	if response.Topics[0].Partitions[0].ErrorCode == 0 {
		t.Fatal("invalid records acknowledged")
	}
}
