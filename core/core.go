package core

import (
	"datareplication_sender/storage/couchbase"
	"net/http"
	"time"
)

type Status int32

var SenderStatus Status = InActiveBasedOnConfigration

const (
	Active Status = iota + 1
	InActiveBecauseOfConflict
	InActiveBasedOnConfigration
	InActiveBecauseOfInternalError
	InActiveReceivers
)

var LastActiveTime time.Time

// Kafka Struct
// --------------------------------------------------------
type KafkaKey_CBtoKafka struct {
	Schema  KafkaKeySchema_CBtoKafka
	Payload string
}

type KafkaKeySchema_CBtoKafka struct {
	Type     string
	Optional bool
}

type KafkaValue_CBtoKafka struct {
	Schema  string
	Payload KafkaValuePayload_CBtoKafka
}

type KafkaValuePayload_CBtoKafka struct {
	Event string
}

//--------------------------------------------------------

// Store the Channel response for
type RoutinesResult struct {
	Error   error
	Message string

	FlagEvent bool

	RtCbTotal     int64
	RtUpdateTotal int64
	RtDeleteTotal int64
}

type Flag int32

const (
	Error Flag = iota + 1
	Success
)

// store the receiver response
type ResponseResult struct {
	Status         Flag
	ErrorMessage   string
	SuccessMessage string
}

// store the metrics observation for the prometheus
type MetricsObservation struct {
	RtConsume      int64
	CountBatch     int
	CountConsume   int
	CountUnique    int
	CountUpdate    int
	CountDelete    int
	RtCbTotal      int64
	RtUpdateTotal  int64
	RtDeleteTotal  int64
	RtCommitOffset int64
	RtTotal        int64
	Topic          string
	Type           string
}

// Store the key & event of document
// it's used for the remove the duplicate and divide into multiple receiver
type KafkaEventPayload struct {
	Payload string
	Event   string
}

type ReceiverMetaData struct {
	ClientObject Receiver
	Payload      string
	Event        string
}

type DocumentMetaData struct {
	Key      string
	Document couchbase.CBRawDataResult
	Bucket   string
}

type Receiver struct {
	ReceiverNo int
	Host       string
	Client     *http.Client
}

type CBConfigDetails struct {
	SenderStatus []SenderStatusStruct `json:"SenderStatus"`
}
type SenderStatusStruct struct {
	Bucket   string `json:"bucketName"`
	IsActive bool   `json:"isActive"`
}
