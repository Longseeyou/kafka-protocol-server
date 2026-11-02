package handlers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type ProduceRequest struct {
	CorrelationID uint32
	Acks          int16
	Topics        []ProduceTopic
}

type ProduceTopic struct {
	Name       string
	Partitions []ProducePartition
}

type ProducePartition struct {
	Index   int32
	Records []byte
}

type ProduceResponse struct {
	Topics []ProduceTopicResponse
}

type ProduceTopicResponse struct {
	Name       string
	Partitions []ProducePartitionResponse
}

type ProducePartitionResponse struct {
	Index           int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
}

// BuildProduceResponse validates each partition within its requested topic.
// PartitionLog.Produce replaces provisional success offsets with append results.
func BuildProduceResponse(request ProduceRequest, metadata TopicMetadata) ProduceResponse {
	var response ProduceResponse
	for _, requestedTopic := range request.Topics {
		topicResponse := ProduceTopicResponse{Name: requestedTopic.Name}
		topic, exists := metadata.Topics[requestedTopic.Name]
		for _, requestedPartition := range requestedTopic.Partitions {
			index := requestedPartition.Index
			partitionResponse := ProducePartitionResponse{
				Index: index, ErrorCode: 3, BaseOffset: -1,
				LogAppendTimeMs: -1, LogStartOffset: -1,
			}
			if exists {
				// Partitions are grouped by topic UUID, so an index belonging
				// to another topic cannot make this request valid.
				for _, partition := range metadata.Partitions[topic.TopicID] {
					if partition.PartitionIndex == index {
						partitionResponse.ErrorCode = 0
						partitionResponse.BaseOffset = 0
						partitionResponse.LogStartOffset = 0
						break
					}
				}
			}
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
		response.Topics = append(response.Topics, topicResponse)
	}
	return response
}

// DecodeProduceRequest reads a v11 request, excluding the four-byte frame size.
// Record batches are retained in their wire format for persistence.
func DecodeProduceRequest(payload []byte) (ProduceRequest, error) {
	var request ProduceRequest
	r := bytes.NewReader(payload)
	header, err := readElement[APIRequestHeader](r)
	if err != nil {
		return request, err
	}
	if header.APIKey != 0 || header.APIVersion != 11 {
		return request, fmt.Errorf("expected Produce v11 request")
	}
	request.CorrelationID = header.CorrelationID

	// Client ID remains a nullable INT16-length string in request header v2.
	clientLength, err := readElement[int16](r)
	if err != nil {
		return request, err
	}
	if clientLength < -1 {
		return request, fmt.Errorf("invalid client ID length: %d", clientLength)
	}
	if clientLength >= 0 {
		if err := skipProduceBytes(r, uint64(clientLength)); err != nil {
			return request, err
		}
	}
	if err := skipProduceTags(r); err != nil {
		return request, err
	}
	transactionLength, err := produceCompactLength(r, true)
	if err != nil {
		return request, err
	}
	if err := skipProduceBytes(r, uint64(transactionLength)); err != nil {
		return request, err
	}
	request.Acks, err = readElement[int16](r)
	if err != nil {
		return request, err
	}
	if _, err := readElement[int32](r); err != nil { // timeout_ms
		return request, err
	}
	topicCount, err := produceCompactLength(r, false)
	if err != nil {
		return request, err
	}
	for i := 0; i < topicCount; i++ {
		nameLength, err := produceCompactLength(r, false)
		if err != nil {
			return request, err
		}
		name := make([]byte, nameLength)
		if _, err := io.ReadFull(r, name); err != nil {
			return request, err
		}
		topic := ProduceTopic{Name: string(name)}
		partitionCount, err := produceCompactLength(r, false)
		if err != nil {
			return request, err
		}
		for j := 0; j < partitionCount; j++ {
			index, err := readElement[int32](r)
			if err != nil {
				return request, err
			}
			recordLength, err := produceCompactLength(r, true)
			if err != nil {
				return request, err
			}
			records := make([]byte, recordLength)
			if _, err := io.ReadFull(r, records); err != nil {
				return request, err
			}
			if err := skipProduceTags(r); err != nil {
				return request, err
			}
			topic.Partitions = append(topic.Partitions, ProducePartition{Index: index, Records: records})
		}
		if err := skipProduceTags(r); err != nil {
			return request, err
		}
		request.Topics = append(request.Topics, topic)
	}
	if err := skipProduceTags(r); err != nil {
		return request, err
	}
	if r.Len() != 0 {
		return request, fmt.Errorf("Produce request has %d trailing bytes", r.Len())
	}
	return request, nil
}

// Compact lengths encode N+1; zero denotes null only for nullable fields.
func produceCompactLength(r *bytes.Reader, nullable bool) (int, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		if nullable {
			return 0, nil
		}
		return 0, fmt.Errorf("unexpected null compact field")
	}
	// Every array element consumes at least one byte, so this bounds counts too.
	if n-1 > uint64(r.Len()) {
		return 0, io.ErrUnexpectedEOF
	}
	return int(n - 1), nil
}

func skipProduceBytes(r *bytes.Reader, n uint64) error {
	if n > uint64(r.Len()) {
		return io.ErrUnexpectedEOF
	}
	_, err := r.Seek(int64(n), io.SeekCurrent)
	return err
}

func skipProduceTags(r *bytes.Reader) error {
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	for i := uint64(0); i < count; i++ {
		if _, err := binary.ReadUvarint(r); err != nil { // tag ID
			return err
		}
		size, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		if err := skipProduceBytes(r, size); err != nil {
			return err
		}
	}
	return nil
}

// EncodeProduceResponse returns a framed Produce v11 response.
func EncodeProduceResponse(header ResponseHeader, response ProduceResponse) []byte {
	payload := binary.BigEndian.AppendUint32(nil, header.CorrelationID)
	payload = append(payload, 0) // response header v1 tagged fields
	payload = binary.AppendUvarint(payload, uint64(len(response.Topics)+1))
	for _, topic := range response.Topics {
		payload = binary.AppendUvarint(payload, uint64(len(topic.Name)+1))
		payload = append(payload, topic.Name...)
		payload = binary.AppendUvarint(payload, uint64(len(topic.Partitions)+1))
		for _, partition := range topic.Partitions {
			payload = binary.BigEndian.AppendUint32(payload, uint32(partition.Index))
			payload = binary.BigEndian.AppendUint16(payload, uint16(partition.ErrorCode))
			payload = binary.BigEndian.AppendUint64(payload, uint64(partition.BaseOffset))
			payload = binary.BigEndian.AppendUint64(payload, uint64(partition.LogAppendTimeMs))
			payload = binary.BigEndian.AppendUint64(payload, uint64(partition.LogStartOffset))
			payload = append(payload, 1, 0, 0) // empty record_errors, null error_message, tags
		}
		payload = append(payload, 0) // topic tags
	}
	payload = binary.BigEndian.AppendUint32(payload, 0) // throttle_time_ms
	payload = append(payload, 0)                        // response tags
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	return append(frame, payload...)
}
