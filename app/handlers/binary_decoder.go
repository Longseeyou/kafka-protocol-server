package handlers

import (
	"bytes"
	"encoding/binary"
	"io"
)

func readCompactString(reader *bytes.Reader) (string, error) {
	length, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	buf := make([]byte, length-1)
	_, err = reader.Read(buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func readElement[T any](reader *bytes.Reader) (T, error) {
	var value T
	err := binary.Read(reader, binary.BigEndian, &value)
	return value, err
}

func readUUID(reader *bytes.Reader) (UUID, error) {
	var value UUID
	if _, err := io.ReadFull(reader, value[:]); err != nil {
		return UUID{}, err
	}
	return value, nil
}

func readCompactArray[T any](
	reader *bytes.Reader,
	readElement func(*bytes.Reader) (T, error),
) ([]T, error) {
	length, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}

	array := make([]T, length-1)
	for i := range array {
		element, err := readElement(reader)
		if err != nil {
			return nil, err
		}
		array[i] = element
	}
	return array, nil
}
