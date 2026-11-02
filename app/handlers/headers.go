package handlers

type ResponseHeader struct {
	CorrelationID uint32
}

type APIRequestHeader struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
}
