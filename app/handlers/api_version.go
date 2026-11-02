package handlers

import (
	"encoding/binary"
)

type APIKey struct {
	APIKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

type APIVersionsResponse struct {
	ErrorCode uint16
	APIKeys   []APIKey
}

func EncodeAPIVersionsResponse(header ResponseHeader, body APIVersionsResponse) []byte {
	payload := make([]byte, 0)
	payload = binary.BigEndian.AppendUint32(payload, header.CorrelationID) // correlation_id
	payload = binary.BigEndian.AppendUint16(payload, body.ErrorCode)       // error_code
	payload = append(payload, byte(len(body.APIKeys)+1))                   // api_keys array length
	for _, key := range body.APIKeys {
		payload = binary.BigEndian.AppendUint16(payload, key.APIKey)     // api_key
		payload = binary.BigEndian.AppendUint16(payload, key.MinVersion) // api_key min_version
		payload = binary.BigEndian.AppendUint16(payload, key.MaxVersion) // api_key max_version
		payload = append(payload, byte(0))                               // TAG_BUFFER
	}
	payload = binary.BigEndian.AppendUint32(payload, 0) // throttle_time_ms
	payload = append(payload, byte(0))                  // TAG_BUFFER

	// Construct the returned frame now that we know size
	frame := make([]byte, 0, len(payload)+4)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	return frame
}

func DecodeAPIversionRequestHeader(payload []byte) APIRequestHeader {
	request := APIRequestHeader{}
	request.APIKey = binary.BigEndian.Uint16(payload[0:2])
	request.APIVersion = binary.BigEndian.Uint16(payload[2:4])
	request.CorrelationID = binary.BigEndian.Uint32(payload[4:8])
	return request
}

func CheckAPIVersion(apiKey uint16, apiVersion uint16) uint16 {
	switch apiKey {
	case 18: // API Versions
		if apiVersion >= 0 && apiVersion <= 4 {
			return 0 // No error
		} else {
			return 35 // UNSUPPORTED_VERSION
		}
	case 75: // DescribeTopicPartitions
		if apiVersion == 0 {
			return 0 // No error
		} else {
			return 35 // UNSUPPORTED_VERSION
		}
	}
	return 0
}
