package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/Longseeyou/kafka-protocol-server/app/handlers"
)

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// configuredMetadata bootstraps stable topic IDs from configured topic names.
func configuredMetadata(topics string) (handlers.ClusterMetadata, error) {
	var records []handlers.Record
	seen := map[string]bool{}
	valid := regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
	for _, entry := range strings.Split(topics, ",") {
		parts := strings.Split(strings.TrimSpace(entry), ":")
		if len(parts) != 2 || !valid.MatchString(parts[0]) || parts[0] == "." || parts[0] == ".." || seen[parts[0]] {
			return handlers.ClusterMetadata{}, fmt.Errorf("invalid or duplicate topic %q; use name:partitions", entry)
		}
		count, err := strconv.Atoi(parts[1])
		if err != nil || count < 1 || count > 1000 {
			return handlers.ClusterMetadata{}, fmt.Errorf("partition count must be 1..1000")
		}
		seen[parts[0]] = true
		hash := sha256.Sum256([]byte(parts[0]))
		var id handlers.UUID
		copy(id[:], hash[:16])
		records = append(records, handlers.Record{Value: handlers.TopicRecord{TopicName: parts[0], TopicUUID: id}})
		for i := 0; i < count; i++ {
			records = append(records, handlers.Record{Value: handlers.PartitionRecord{TopicUUID: id, PartitionID: int32(i), Leader: 1, Replicas: []int32{1}, InSyncReplicas: []int32{1}}})
		}
	}
	return handlers.ClusterMetadata{RecordBatches: []handlers.RecordBatch{{Records: records}}}, nil
}
