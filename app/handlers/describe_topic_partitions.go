package handlers

import (
	"cmp"
	"encoding/binary"
	"slices"
)

type UUID [16]byte

type Topic struct {
	ErrorCode            uint16
	TopicName            string
	TopicID              UUID
	IsInternal           bool
	Partitions           []Partition
	AuthorizedOperations int32
}

type Partition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderID               int32
	LeaderEpoch            int32
	ReplicaNodes           []int32
	ISRNodes               []int32
	EligibleLeaderReplicas []int32
	LastKnownELR           []int32
	OfflineReplicas        []int32
}

type DescribeTopicPartitionsRequest struct {
	Topics []Topic
}

type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMs int32
	Topics         []Topic
	NextCursor     int8
}

type TopicCursor struct {
	TopicName      string
	PartitionIndex int32
}

func EncodeDescribeTopicPartitionsResponse(header ResponseHeader, body DescribeTopicPartitionsResponse) []byte {
	// Responses must be alphabetical regardless of the request's topic order.
	topics := slices.Clone(body.Topics)
	slices.SortFunc(topics, func(a, b Topic) int {
		return cmp.Compare(a.TopicName, b.TopicName)
	})
	payload := make([]byte, 0)
	payload = binary.BigEndian.AppendUint32(payload, header.CorrelationID) // correlation_id
	payload = append(payload, byte(0))                                     // TAG_BUFFER
	payload = binary.BigEndian.AppendUint32(payload, 0)                    // throttle_time_ms
	payload = append(payload, byte(len(body.Topics)+1))                    // topics array length
	for _, topic := range topics {
		payload = binary.BigEndian.AppendUint16(payload, topic.ErrorCode) // error_code (UNKNOWN_TOPIC_OR_PARTITION)
		payload = append(payload, byte(len(topic.TopicName)+1))           // topic_name length
		payload = append(payload, []byte(topic.TopicName)...)             // topic_name
		payload = append(payload, topic.TopicID[:]...)                    // topic_id
		payload = append(payload, byte(0))                                // is_internal
		payload = append(payload, byte(len(topic.Partitions)+1))          // partitions array
		for _, partition := range topic.Partitions {
			payload, _ = binary.Append(payload, binary.BigEndian, partition.ErrorCode)                  // error_code
			payload, _ = binary.Append(payload, binary.BigEndian, partition.PartitionIndex)             // partitionID
			payload, _ = binary.Append(payload, binary.BigEndian, partition.LeaderID)                   // leaderID
			payload, _ = binary.Append(payload, binary.BigEndian, partition.LeaderEpoch)                // leaderEpoch
			payload, _ = binary.Append(payload, binary.BigEndian, uint8(len(partition.ReplicaNodes)+1)) // replica_nodes len
			payload, _ = binary.Append(payload, binary.BigEndian, partition.ReplicaNodes)               // replica_array
			payload, _ = binary.Append(payload, binary.BigEndian, uint8(len(partition.ISRNodes)+1))     // ISR_nodes
			payload, _ = binary.Append(payload, binary.BigEndian, partition.ISRNodes)
			payload = append(payload, byte(1)) // ELR
			payload = append(payload, byte(1)) // last_know_ELR
			payload = append(payload, byte(1)) // offline_replicas
			payload = append(payload, byte(0)) // TAG_BUFFER
		}
		payload = binary.BigEndian.AppendUint32(payload, 0) // authorized_operations
		payload = append(payload, byte(0))                  // TAG_BUFFER
	}
	payload = append(payload, byte(0xff)) // next_cursor (-1)
	payload = append(payload, byte(0))    // TAG_BUFFER

	frame := make([]byte, 0, len(payload)+4)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	return frame
}

func DecodeDescribeTopicPartitionsRequest(payload []byte, topicMetadata TopicMetadata) DescribeTopicPartitionsRequest {
	request := DescribeTopicPartitionsRequest{}
	currentIndex := 8 // Start after the API request header (8 bytes)
	clientIDLength := binary.BigEndian.Uint16(payload[currentIndex : currentIndex+2])
	currentIndex += 2 + int(clientIDLength)       // Move index past client_id
	currentIndex++                                // skip TAG_BUFFER
	arrayLength := int(payload[currentIndex]) - 1 // topics array length
	currentIndex++

	for i := 0; i < arrayLength; i++ {
		topicNameLength := int(payload[currentIndex]) - 1 // topic name length
		currentIndex++

		topicName := string(payload[currentIndex : currentIndex+topicNameLength])

		currentIndex += topicNameLength
		topic, ok := topicMetadata.Topics[topicName]
		if !ok {
			topic = Topic{
				ErrorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
				TopicName: topicName,
			}
		}
		request.Topics = append(request.Topics, topic)
		currentIndex++ // Skip TAG_BUFFER for each topic

	}
	return request
}
