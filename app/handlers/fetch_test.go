package handlers

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFetchRecordsFromDisk(t *testing.T) {
	root := t.TempDir()
	id := UUID{42}
	metadata := TopicMetadata{
		Topics:     map[string]Topic{"fruit": {TopicID: id}},
		Partitions: map[UUID][]Partition{id: {{PartitionIndex: 0}, {PartitionIndex: 2}}},
	}
	request := FetchRequest{Topics: []FetchTopic{{TopicID: id, Partitions: []FetchPartition{{Index: 0}}}}}
	path := filepath.Join(root, "fruit-0", "00000000000000000000.log")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"Hello Kafka!", strings.Repeat("x", 200)} {
		t.Run(value[:5], func(t *testing.T) {
			batch := testBatch(value)
			if err := os.WriteFile(path, batch, 0644); err != nil {
				t.Fatal(err)
			}
			response := NewPartitionLog(root).Fetch(request, metadata)
			if p := response[0].Partitions[0]; p.ErrorCode != 0 || !bytes.Equal(p.Records, batch) {
				t.Fatalf("partition = %+v, want original disk bytes", p)
			}
			frame := EncodeFetchResponse(ResponseHeader{CorrelationID: 99}, response)
			if int(binary.BigEndian.Uint32(frame)) != len(frame)-4 || binary.BigEndian.Uint32(frame[4:]) != 99 {
				t.Fatal("incorrect frame length or correlation ID")
			}
			// Header (5), body prefix (11), topic ID/count (17), partition fields (35).
			r := bytes.NewReader(frame[4+5+11+17+35:])
			length, err := binary.ReadUvarint(r)
			if err != nil || length != uint64(len(batch)+1) {
				t.Fatalf("compact records length = %d, %v", length, err)
			}
			got := make([]byte, len(batch))
			if _, err := io.ReadFull(r, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, batch) {
				t.Fatal("wire record batch differs from disk, including CRC")
			}
			tags, err := io.ReadAll(r)
			if err != nil || !bytes.Equal(tags, []byte{0, 0, 0}) {
				t.Fatalf("trailing tags = %x, %v", tags, err)
			}
		})
	}

	request.Topics[0].Partitions = []FetchPartition{{Index: 2}, {Index: 7}}
	request.Topics = append(request.Topics, FetchTopic{TopicID: UUID{99}, Partitions: []FetchPartition{{Index: 0}}})
	response := NewPartitionLog(root).Fetch(request, metadata)
	if p := response[0].Partitions[0]; p.ErrorCode != 0 || len(p.Records) != 0 {
		t.Fatalf("missing log should be empty: %+v", p)
	}
	if response[0].Partitions[1].ErrorCode != 3 || response[1].Partitions[0].ErrorCode != 100 {
		t.Fatalf("validation errors lost: %+v", response)
	}

	// A filesystem read failure must not become a successful empty response.
	if err := os.MkdirAll(filepath.Join(root, "fruit-2", "00000000000000000000.log"), 0755); err != nil {
		t.Fatal(err)
	}
	response = NewPartitionLog(root).Fetch(request, metadata)
	if response[0].Partitions[0].ErrorCode != -1 {
		t.Fatal("log read failure was not reported")
	}
}

func fetchFixture() []byte {
	b := binary.BigEndian.AppendUint16(nil, 1)
	b = binary.BigEndian.AppendUint16(b, 16)
	b = binary.BigEndian.AppendUint32(b, 99)
	b = append(b, 0, 3, 'a', 'b', 'c', 1, 7, 2, 8, 9) // client ID; one unknown header tag
	b = append(b, make([]byte, 21)...)
	b = append(b, 3) // two topics
	for _, id := range []byte{11, 22} {
		b = append(b, bytes.Repeat([]byte{id}, 16)...)
		b = append(b, 3) // two partitions
		for _, index := range []uint32{2, 7} {
			b = binary.BigEndian.AppendUint32(b, index)
			b = append(b, make([]byte, 28)...)
			b = append(b, 1, 5, 1, 42) // partition tag
		}
		b = append(b, 0)
	}
	b = append(b, 2) // one forgotten topic
	b = append(b, make([]byte, 16)...)
	b = append(b, 2, 0, 0, 0, 3, 0)        // one forgotten partition and tags
	b = append(b, 3, 'r', '1', 1, 4, 1, 9) // rack and body tag
	return b
}

func TestFetchEmptyTopicPartitions(t *testing.T) {
	id, otherID, unknownID := UUID{1}, UUID{2}, UUID{3}
	metadata := TopicMetadata{
		Topics: map[string]Topic{"empty": {TopicID: id}, "other": {TopicID: otherID}},
		Partitions: map[UUID][]Partition{
			id:      {{PartitionIndex: 0}, {PartitionIndex: 2}, {PartitionIndex: 5}},
			otherID: {{PartitionIndex: 7}},
		},
	}
	request := FetchRequest{Topics: []FetchTopic{
		{TopicID: id, Partitions: []FetchPartition{{Index: 2}, {Index: 0}, {Index: 7}}},
		{TopicID: unknownID, Partitions: []FetchPartition{{Index: 0}}},
	}}
	want := []FetchTopicResponse{
		{TopicID: id, Partitions: []FetchPartitionResponse{{Index: 2}, {Index: 0}, {Index: 7, ErrorCode: 3}}},
		{TopicID: unknownID, Partitions: []FetchPartitionResponse{{Index: 0, ErrorCode: 100}}},
	}
	if got := BuildFetchResponse(request, metadata); !reflect.DeepEqual(got, want) {
		t.Fatalf("response = %+v, want %+v", got, want)
	}

	// The existing partition must be included even though its records are empty.
	request.Topics = []FetchTopic{{TopicID: id, Partitions: []FetchPartition{{Index: 2}}}}
	frame := EncodeFetchResponse(ResponseHeader{CorrelationID: 42}, BuildFetchResponse(request, metadata))
	wantFrame := []byte{
		0, 0, 0, 72, // frame payload length
		0, 0, 0, 42, 0, // correlation ID, header tags
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // throttle, error, session
		2, // one topic
	}
	wantFrame = append(wantFrame, id[:]...)
	wantFrame = append(wantFrame, 2, 0, 0, 0, 2, 0, 0)               // one partition, index 2, success
	wantFrame = append(wantFrame, make([]byte, 24)...)               // empty-log offsets
	wantFrame = append(wantFrame, 1, 255, 255, 255, 255, 1, 0, 0, 0) // aborted, replica, records, tags
	if !bytes.Equal(frame, wantFrame) {
		t.Fatalf("frame = %x, want %x", frame, wantFrame)
	}
}

func TestUnknownFetchTopics(t *testing.T) {
	request, err := DecodeFetchRequest(fetchFixture())
	if err != nil {
		t.Fatal(err)
	}
	if request.CorrelationID != 99 || len(request.Topics) != 2 {
		t.Fatalf("%+v", request)
	}
	response := EncodeFetchResponse(ResponseHeader{CorrelationID: 99}, BuildFetchResponse(request, TopicMetadata{}))
	if int(binary.BigEndian.Uint32(response)) != len(response)-4 {
		t.Fatal("bad frame length")
	}
	r := bytes.NewReader(response[4:])
	var header struct {
		Correlation uint32
		Tags        byte
		Throttle    int32
		Error       int16
		Session     int32
	}
	if err := binary.Read(r, binary.BigEndian, &header); err != nil {
		t.Fatal(err)
	}
	if header.Correlation != 99 || header.Error != 0 || header.Tags != 0 {
		t.Fatal(header)
	}
	count, _ := binary.ReadUvarint(r)
	if count != 3 {
		t.Fatal(count)
	}
	for _, id := range []byte{11, 22} {
		var uuid UUID
		if err := binary.Read(r, binary.BigEndian, &uuid); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(uuid[:], bytes.Repeat([]byte{id}, 16)) {
			t.Fatal(uuid)
		}
		count, _ = binary.ReadUvarint(r)
		if count != 3 {
			t.Fatal(count)
		}
		for _, index := range []int32{2, 7} {
			var p struct {
				Index   int32
				Error   int16
				High    int64
				Stable  int64
				Start   int64
				Aborted byte
				Replica int32
				Records byte
				Tags    byte
			}
			if err := binary.Read(r, binary.BigEndian, &p); err != nil {
				t.Fatal(err)
			}
			if p.Index != index || p.Error != 100 || p.Aborted != 1 || p.Replica != -1 || p.Records != 1 || p.Tags != 0 {
				t.Fatalf("%+v", p)
			}
		}
		tag, _ := r.ReadByte()
		if tag != 0 {
			t.Fatal(tag)
		}
	}
	tag, _ := r.ReadByte()
	if tag != 0 || r.Len() != 0 {
		t.Fatal("invalid trailing fields")
	}
}

func TestFetchRejectsTruncation(t *testing.T) {
	payload := fetchFixture()
	for i := 0; i < len(payload); i++ {
		if _, err := DecodeFetchRequest(payload[:i]); err == nil {
			t.Fatalf("accepted prefix of length %d", i)
		}
	}
	if _, err := DecodeFetchRequest(append(payload, 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	payload[3] = 15
	if _, err := DecodeFetchRequest(payload); err == nil {
		t.Fatal("accepted wrong version")
	}
}
