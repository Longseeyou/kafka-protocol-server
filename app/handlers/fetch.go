package handlers

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// EncodeEmptyFetchResponse returns a framed Fetch v16 response with no topics.
// Fetch uses response header v1, including a tagged-fields buffer.
func EncodeEmptyFetchResponse(header ResponseHeader) []byte {
	return EncodeFetchResponse(header, nil)
}

func EncodeFetchResponse(header ResponseHeader, topics []FetchTopicResponse) []byte {
	payload := binary.BigEndian.AppendUint32(nil, header.CorrelationID)
	payload = append(payload, 0)                        // header tags
	payload = binary.BigEndian.AppendUint32(payload, 0) // throttle_time_ms
	payload = binary.BigEndian.AppendUint16(payload, 0) // error_code
	payload = binary.BigEndian.AppendUint32(payload, 0) // session_id
	payload = binary.AppendUvarint(payload, uint64(len(topics)+1))
	for _, topic := range topics {
		payload = append(payload, topic.TopicID[:]...)
		payload = binary.AppendUvarint(payload, uint64(len(topic.Partitions)+1))
		for _, partition := range topic.Partitions {
			payload = binary.BigEndian.AppendUint32(payload, uint32(partition.Index))
			payload = binary.BigEndian.AppendUint16(payload, uint16(partition.ErrorCode))
			payload = binary.BigEndian.AppendUint64(payload, 0)          // high watermark
			payload = binary.BigEndian.AppendUint64(payload, 0)          // last stable offset
			payload = binary.BigEndian.AppendUint64(payload, 0)          // log start offset
			payload = append(payload, 1)                                 // empty aborted transactions
			payload = binary.BigEndian.AppendUint32(payload, 0xffffffff) // no preferred replica
			payload = binary.AppendUvarint(payload, uint64(len(partition.Records)+1))
			payload = append(payload, partition.Records...)
			payload = append(payload, 0) // partition tags
		}
		payload = append(payload, 0) // topic tags
	}
	payload = append(payload, 0) // body tags
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	return append(frame, payload...)
}

type FetchRequest struct {
	CorrelationID uint32
	Topics        []FetchTopic
}
type FetchTopic struct {
	TopicID    UUID
	Partitions []FetchPartition
}
type FetchPartition struct {
	Index              int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}
type FetchTopicResponse struct {
	TopicID    UUID
	Partitions []FetchPartitionResponse
}
type FetchPartitionResponse struct {
	Index     int32
	ErrorCode int16
	Records   []byte
}

// DecodeFetchRequest parses header v2 and Fetch v16, excluding frame size.
func DecodeFetchRequest(payload []byte) (FetchRequest, error) {
	var request FetchRequest
	r := bytes.NewReader(payload)
	header, err := readElement[APIRequestHeader](r)
	if err != nil {
		return request, err
	}
	if header.APIKey != 1 || header.APIVersion != 16 {
		return request, fmt.Errorf("expected Fetch v16")
	}
	request.CorrelationID = header.CorrelationID
	clientLength, err := readElement[int16](r)
	if err != nil {
		return request, err
	}
	if clientLength < -1 {
		return request, fmt.Errorf("invalid client ID length")
	}
	if clientLength >= 0 {
		if err := skipProduceBytes(r, uint64(clientLength)); err != nil {
			return request, err
		}
	}
	if err := skipProduceTags(r); err != nil {
		return request, err
	}
	// max_wait_ms, min_bytes, max_bytes, isolation_level, session_id, session_epoch.
	// Replica ID is a tagged field in v16, not an inline INT32.
	if err := skipProduceBytes(r, 21); err != nil {
		return request, err
	}
	count, err := produceCompactLength(r, false)
	if err != nil {
		return request, err
	}
	for i := 0; i < count; i++ {
		id, err := readUUID(r)
		if err != nil {
			return request, err
		}
		topic := FetchTopic{TopicID: id}
		partitions, err := produceCompactLength(r, false)
		if err != nil {
			return request, err
		}
		for j := 0; j < partitions; j++ {
			partition, err := readElement[FetchPartition](r)
			if err != nil {
				return request, err
			}
			if err := skipProduceTags(r); err != nil {
				return request, err
			}
			topic.Partitions = append(topic.Partitions, partition)
		}
		if err := skipProduceTags(r); err != nil {
			return request, err
		}
		request.Topics = append(request.Topics, topic)
	}
	forgotten, err := produceCompactLength(r, false)
	if err != nil {
		return request, err
	}
	for i := 0; i < forgotten; i++ {
		if _, err := readUUID(r); err != nil {
			return request, err
		}
		count, err := produceCompactLength(r, false)
		if err != nil {
			return request, err
		}
		if err := skipProduceBytes(r, uint64(count)*4); err != nil {
			return request, err
		}
		if err := skipProduceTags(r); err != nil {
			return request, err
		}
	}
	rackLength, err := produceCompactLength(r, false)
	if err != nil {
		return request, err
	}
	if err := skipProduceBytes(r, uint64(rackLength)); err != nil {
		return request, err
	}
	if err := skipProduceTags(r); err != nil {
		return request, err
	}
	if r.Len() != 0 {
		return request, fmt.Errorf("Fetch request has trailing bytes")
	}
	return request, nil
}

// BuildFetchResponse validates topic IDs and requested partitions.
func BuildFetchResponse(request FetchRequest, metadata TopicMetadata) []FetchTopicResponse {
	var topics []FetchTopicResponse
	for _, requested := range request.Topics {
		known := false
		for _, topic := range metadata.Topics {
			if topic.TopicID == requested.TopicID {
				known = true
				break
			}
		}
		topic := FetchTopicResponse{TopicID: requested.TopicID}
		for _, partition := range requested.Partitions {
			code := int16(100) // UNKNOWN_TOPIC_ID
			if known {
				code = 3 // UNKNOWN_TOPIC_OR_PARTITION
				for _, existing := range metadata.Partitions[requested.TopicID] {
					if existing.PartitionIndex == partition.Index {
						code = 0
						break
					}
				}
			}
			topic.Partitions = append(topic.Partitions, FetchPartitionResponse{Index: partition.Index, ErrorCode: code})
		}
		topics = append(topics, topic)
	}
	return topics
}

// Fetch returns stored record batches unchanged, including their original CRCs.
func (s *PartitionLog) Fetch(request FetchRequest, metadata TopicMetadata) []FetchTopicResponse {
	response := BuildFetchResponse(request, metadata)
	names := make(map[UUID]string, len(metadata.Topics))
	for name, topic := range metadata.Topics {
		names[topic.TopicID] = name
	}
	for i := range response {
		for j := range response[i].Partitions {
			partition := &response[i].Partitions[j]
			if partition.ErrorCode != 0 {
				continue
			}
			records, err := s.Read(names[response[i].TopicID], partition.Index)
			if err != nil {
				partition.ErrorCode = -1 // UNKNOWN_SERVER_ERROR
				continue
			}
			partition.Records = records
		}
	}
	return response
}
