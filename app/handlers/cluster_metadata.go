package handlers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	topicRecordType     = 2
	partitionRecordType = 3
	featureRecordType   = 12
)

type RecordHeader struct {
	FrameVersion uint8
	Type         uint8
	Version      uint8
}

type PartitionRecord struct {
	Header                        RecordHeader
	PartitionID                   int32
	TopicUUID                     UUID
	LengthOfReplicasArray         uint8
	Replicas                      []int32
	LengthOfInSyncReplicaArray    uint8
	InSyncReplicas                []int32
	LengthOfRemovingReplicasArray int8
	RemovingReplicasArray         []int32
	LengthOfAddingReplicasArray   int8
	AddingReplicasArray           []int32
	Leader                        int32
	LeaderEpoch                   int32
	PartitionEpoch                int32
	LengthOfDirectoriesArray      uint8
	DirectoriesArray              []UUID
	TaggedFieldsCount             uint8
}

type TopicRecord struct {
	Header            RecordHeader
	TopicName         string
	TopicUUID         UUID
	TaggedFieldsCount uint8 // always 0 this case
}

type FeatureRecord struct {
	Header            RecordHeader
	Name              string
	FeatureLevel      int16
	TaggedFieldsCount uint8 // always 0 this case
}

type TopicMetadata struct {
	Topics     map[string]Topic
	Partitions map[UUID][]Partition
}

type Record struct {
	Length            int64
	Attribute         int8
	TimestampDelta    int64
	OffsetDelta       int64
	Key               []byte
	ValueLength       int64
	Value             any
	HeadersArrayCount int64
}

type RecordBatchHeader struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
}

type RecordBatch struct {
	Header  RecordBatchHeader
	Records []Record
}

type ClusterMetadata struct {
	RecordBatches []RecordBatch
}

func DecodeClusterMetadataRequest(payload []byte) (ClusterMetadata, error) {
	cluster := ClusterMetadata{}

	for len(payload) > 0 {
		if len(payload) < 12 {
			return ClusterMetadata{}, fmt.Errorf("truncated batch prefix")
		}

		batchLength := int32(binary.BigEndian.Uint32(payload[8:12]))

		// 49 bytes remain in the fixed header, including record count.
		if batchLength < 49 || int64(batchLength) > int64(len(payload)-12) {
			return ClusterMetadata{}, fmt.Errorf(
				"invalid batch length: %d", batchLength,
			)
		}

		batchSize := 12 + int(batchLength)
		reader := bytes.NewReader(payload[:batchSize])

		var batch RecordBatch
		if _, err := batch.readRecordBatch(reader); err != nil {
			return ClusterMetadata{}, err
		}
		if reader.Len() != 0 {
			return ClusterMetadata{}, fmt.Errorf(
				"batch has %d unconsumed bytes", reader.Len(),
			)
		}
		cluster.RecordBatches = append(cluster.RecordBatches, batch)
		payload = payload[batchSize:]
	}
	return cluster, nil
}

func (rb *RecordBatch) readRecordBatch(reader *bytes.Reader) (RecordBatch, error) {
	if err := binary.Read(reader, binary.BigEndian, &rb.Header); err != nil {
		return RecordBatch{}, err
	}

	recordCount, err := readElement[int32](reader)
	if err != nil {
		return RecordBatch{}, err
	}
	if recordCount < 0 {
		return RecordBatch{}, fmt.Errorf("invalid record count: %d", recordCount)
	}

	rb.Records = make([]Record, 0, recordCount)
	for i := int32(0); i < recordCount; i++ {
		record, err := readRecord(reader)
		if err != nil {
			return RecordBatch{}, err
		}
		rb.Records = append(rb.Records, record)
	}

	return *rb, nil
}

func (pr *PartitionRecord) readPartitionRecord(reader *bytes.Reader) (PartitionRecord, error) {
	record := PartitionRecord{}

	PartitionID, err := readElement[int32](reader)
	if err != nil {
		return PartitionRecord{}, err
	}
	record.PartitionID = PartitionID

	TopicUUID, err := readUUID(reader)
	if err != nil {
		return PartitionRecord{}, err
	}
	record.TopicUUID = TopicUUID

	Replicas, err := readCompactArray(reader, readElement[int32])
	if err != nil {
		return PartitionRecord{}, err
	}
	record.Replicas = Replicas

	InSyncReplicas, err := readCompactArray(reader, readElement[int32])
	if err != nil {
		return PartitionRecord{}, err
	}
	record.InSyncReplicas = InSyncReplicas

	// Removing replicas
	if _, err := readCompactArray(reader, readElement[int32]); err != nil {
		return PartitionRecord{}, err
	}

	// Adding replicas
	if _, err := readCompactArray(reader, readElement[int32]); err != nil {
		return PartitionRecord{}, err
	}

	Leader, err := readElement[int32](reader)
	if err != nil {
		return PartitionRecord{}, err
	}
	record.Leader = Leader

	LeaderEpoch, err := readElement[int32](reader)
	if err != nil {
		return PartitionRecord{}, err
	}
	record.LeaderEpoch = LeaderEpoch

	PartitionEpoch, err := readElement[int32](reader)
	if err != nil {
		return PartitionRecord{}, err
	}
	record.PartitionEpoch = PartitionEpoch

	DirectoriesArray, err := readCompactArray(reader, readUUID)
	if err != nil {
		return PartitionRecord{}, err
	}
	record.DirectoriesArray = DirectoriesArray

	TaggedFieldsCount, err := readElement[uint8](reader)
	if err != nil {
		return PartitionRecord{}, err
	}
	record.TaggedFieldsCount = TaggedFieldsCount

	*pr = record

	return record, nil
}

func (rc *RecordHeader) ReadRecordHeader(reader *bytes.Reader) (RecordHeader, error) {
	header := RecordHeader{}

	if err := binary.Read(reader, binary.BigEndian, &header.FrameVersion); err != nil {
		fmt.Println("failed to read RecordHeader.FrameVersion")
		return RecordHeader{}, err
	}

	if err := binary.Read(reader, binary.BigEndian, &header.Type); err != nil {
		fmt.Println("failed to read RecordHeader.Type")
		return RecordHeader{}, err
	}

	if err := binary.Read(reader, binary.BigEndian, &header.Version); err != nil {
		fmt.Println("failed to read RecordHeader.Verstion")
		return RecordHeader{}, err
	}

	*rc = header
	return header, nil
}

func (tr *TopicRecord) readTopicRecord(reader *bytes.Reader) (TopicRecord, error) {
	record := TopicRecord{}

	TopicName, err := readCompactString(reader)
	if err != nil {
		return TopicRecord{}, err
	}
	record.TopicName = TopicName

	TopicUUID, err := readUUID(reader)
	if err != nil {
		return TopicRecord{}, err
	}
	record.TopicUUID = TopicUUID

	TaggedFieldsCount, err := readElement[uint8](reader)
	if err != nil {
		return TopicRecord{}, err
	}
	record.TaggedFieldsCount = TaggedFieldsCount

	*tr = record
	return record, nil
}

func (fr *FeatureRecord) readFeatureRecord(reader *bytes.Reader) (FeatureRecord, error) {
	record := FeatureRecord{}

	Name, err := readCompactString(reader)
	if err != nil {
		return FeatureRecord{}, err
	}
	record.Name = Name

	FeatureLevel, err := readElement[int16](reader)
	if err != nil {
		return FeatureRecord{}, err
	}
	record.FeatureLevel = FeatureLevel

	TaggedFieldsCount, err := readElement[uint8](reader)
	if err != nil {
		return FeatureRecord{}, err
	}
	record.TaggedFieldsCount = TaggedFieldsCount

	*fr = record
	return record, nil
}

func readRecord(reader *bytes.Reader) (Record, error) {
	record := Record{}

	Length, err := binary.ReadVarint(reader)
	if err != nil {
		return Record{}, err
	}
	record.Length = Length

	Attribute, err := readElement[int8](reader)
	if err != nil {
		return Record{}, err
	}
	record.Attribute = Attribute

	TimestampDelta, err := binary.ReadVarint(reader)
	if err != nil {
		return Record{}, err
	}
	record.TimestampDelta = TimestampDelta

	OffsetDelta, err := binary.ReadVarint(reader)
	if err != nil {
		return Record{}, err
	}
	record.OffsetDelta = OffsetDelta

	keyLength, err := binary.ReadVarint(reader)
	if err != nil {
		return Record{}, err
	}
	if keyLength < -1 || keyLength > int64(reader.Len()) {
		return Record{}, fmt.Errorf("invalid key length: %d", keyLength)
	}
	if keyLength >= 0 {
		record.Key = make([]byte, int(keyLength))
		if _, err := io.ReadFull(reader, record.Key); err != nil {
			return Record{}, err
		}
	}

	ValueLength, err := binary.ReadVarint(reader)
	if err != nil {
		return Record{}, err
	}
	record.ValueLength = ValueLength

	var header RecordHeader
	if _, err := header.ReadRecordHeader(reader); err != nil {
		return Record{}, err
	}

	switch header.Type {
	case topicRecordType:
		var typeRecord TopicRecord
		_, err := typeRecord.readTopicRecord(reader)
		if err != nil {
			return Record{}, err
		}
		typeRecord.Header = header
		record.Value = typeRecord
	case partitionRecordType:
		var typeRecord PartitionRecord
		_, err := typeRecord.readPartitionRecord(reader)
		if err != nil {
			return Record{}, err
		}
		typeRecord.Header = header
		record.Value = typeRecord

	case featureRecordType:
		var typeRecord FeatureRecord
		_, err := typeRecord.readFeatureRecord(reader)
		if err != nil {
			return Record{}, err
		}
		typeRecord.Header = header
		record.Value = typeRecord
	}

	HeadersArrayCount, err := binary.ReadVarint(reader)
	if err != nil {
		return Record{}, err
	}
	record.HeadersArrayCount = HeadersArrayCount

	return record, nil
}
