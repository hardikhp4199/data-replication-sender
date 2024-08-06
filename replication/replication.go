package replication

import (
	"datareplication_sender/config"
	"datareplication_sender/core"
	"datareplication_sender/storage/couchbase"
	"datareplication_sender/storage/kafka"
	"datareplication_sender/storage/logging"
	"datareplication_sender/util/common"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
)

var (
	topic                                     = config.GetString("Kafka.Topics.SourceTopic")
	metricsTopic                              = config.GetString("Kafka.Topics.MetricsTopic")
	consumerGroup                             = config.GetString("Kafka.ConsumerGroup")
	batch                                     = config.GetInt("KafkaBatchSize")
	sourceBucket                              = config.GetString("Couchbase.Buckets.SourceBucket")
	destinationBucket                         = config.GetString("Couchbase.Buckets.DestinationBucket")
	cbConfigDocKey                            = config.GetString("Couchbase.CBConfigDocKey")
	cbConfigBucket                            = config.GetString("Couchbase.CBConfigBucket")
	receiverDomains                           = config.GetString("Receiver.Host")
	upsertEndpoint                            = config.GetString("Receiver.Endpoints.Upsert")
	removeEndpoint                            = config.GetString("Receiver.Endpoints.Remove")
	receiverGlobalConnections                 = make(map[int]core.Receiver)
	receiverDomainsArray                      = strings.Split(receiverDomains, ",")
	kafkaMessageKey                           = config.GetString("Metrics.KafkaMessageKey")
	typeName                                  = config.GetString("Metrics.LabelType")
	batchIntervalTime                         = config.GetInt64("WaitBetweenBatches_MiliSecond")
	keepDockeyPrefix                          = config.GetString("KeepDockeyPrefix")
	skipDockeyPrefix                          = config.GetString("SkipDockeyPrefix")
	sleepTimeBeforeNextCheckForInActiveSender = config.GetInt64("SleepTimeBeforeNextCheckForInActiveSenderInSeconds")
)

// intialize all client and call the receiver helthcheck
func InitializeReceiverClients() (errIntialize []error) {

	for i, domain := range receiverDomainsArray {
		httpObject, tranportObjectErr := common.GetTranportObject()
		if tranportObjectErr != nil {
			errIntialize = append(errIntialize, tranportObjectErr)
		} else {
			receiver := core.Receiver{
				ReceiverNo: i,
				Host:       domain,
				Client:     &http.Client{Transport: httpObject},
			}
			errHealthCheck := healthCheck(receiver)
			if errHealthCheck == nil {
				receiverGlobalConnections[i] = receiver
			} else {
				errIntialize = append(errIntialize, errHealthCheck)
			}
		}
	}
	return errIntialize
}

// start replication
func StartReplication() {
	processBatch()
}

// recursive function for the source to destination
func processBatch() {
	core.LastActiveTime = time.Now().UTC()

	metrics := core.MetricsObservation{}

	// batch process time measurement
	startBatch := time.Now()

	// Consume message from Kafka time
	rtConsumeStart := time.Now()

	consumeMessageList, err_consume := consumeMessage(batch)

	metrics.RtConsume = time.Since(rtConsumeStart).Milliseconds()

	//check the any occur in batch
	errorInPuttingDataToReceiver := false

	//main batch obs
	if err_consume != nil {
		logging.DoLoggingLevelBasedLogs(logging.Error, "", err_consume)
	} else {
		// actual length of batch observation
		metrics.CountConsume = len(consumeMessageList)

		collectRoutinesResult := []core.RoutinesResult{} // collects channel response
		// Chack the consume message length is > 0
		if len(consumeMessageList) > 0 {
			var rtCbTotal, rtUpdateTotal, rtDeleteTotal int64
			var rtUpdateCount, rtDeleteCount int

			uniqueMessages := removeDuplicate(consumeMessageList)
			filteredMessage := filterMessages(uniqueMessages)

			logging.DoLoggingLevelBasedLogs(logging.Debug, "unique count: "+strconv.Itoa(len(uniqueMessages))+" filtered message count: "+strconv.Itoa(len(filteredMessage)), nil)

			metrics.CountUnique = len(filteredMessage)

			messageBatchs := makeMessageBatch(filteredMessage) //Divide the documnet's id by receivers

			responseChannel := make(chan core.RoutinesResult) // channel init
			for _, receiverMetaData := range messageBatchs {
				go getAndSendDocument(receiverMetaData, responseChannel)
			}
			//get back the channel response
			for i := 0; i < len(messageBatchs); i++ {
				channelRes := <-responseChannel
				if channelRes.Error != nil {
					errorInPuttingDataToReceiver = true
					logging.DoLoggingLevelBasedLogs(logging.Error, "", channelRes.Error)
				} else {
					rtCbTotal = rtCbTotal + channelRes.RtCbTotal
					rtUpdateTotal = rtUpdateTotal + channelRes.RtUpdateTotal
					rtDeleteTotal = rtDeleteTotal + channelRes.RtDeleteTotal
					if channelRes.FlagEvent {
						rtUpdateCount = rtUpdateCount + 1
					} else {
						rtDeleteCount = rtDeleteCount + 1
					}
				}
				collectRoutinesResult = append(collectRoutinesResult, channelRes)
			}
			metrics.RtCbTotal = rtCbTotal
			metrics.RtUpdateTotal = rtUpdateTotal
			metrics.RtDeleteTotal = rtDeleteTotal
			metrics.CountUpdate = rtUpdateCount
			metrics.CountDelete = rtDeleteCount

			// Error checking
			if errorInPuttingDataToReceiver {
				for _, v := range collectRoutinesResult {
					if v.Error != nil {
						logging.DoLoggingLevelBasedLogs(logging.Error, "", v.Error)
					}
				}
			} else {
				startCommitTime := time.Now()
				// kafka commit offset
				errCommitOffset := kafka.CommitOffset(topic, consumerGroup)
				if errCommitOffset != nil {
					logging.DoLoggingLevelBasedLogs(logging.Error, "", logging.EnrichErrorWithStackTrace(errCommitOffset))
				} else {
					logging.DoLoggingLevelBasedLogs(logging.Debug, "commit offset successfully on kafka", nil)
					endCommitTime := time.Since(startCommitTime).Milliseconds()
					metrics.RtCommitOffset = endCommitTime
					// ending batch process time
					metrics.RtTotal = time.Since(startBatch).Milliseconds()

					err_metrics := produceMetrics(metrics)
					if err_metrics != nil {
						logging.DoLoggingLevelBasedLogs(logging.Error, "", err_metrics)
					}
				}
			}
		}
		logging.DoLoggingLevelBasedLogs(logging.Debug, "ErrorInPuttingDataToReceiver status: "+strconv.FormatBool(errorInPuttingDataToReceiver), nil)
		if !errorInPuttingDataToReceiver {
			time.Sleep(time.Duration(batchIntervalTime) * time.Millisecond)
			processBatch()
		}
	}
}

// consume the message from the kafka and return the customer id list
func consumeMessage(batch int) (KafkaKeyValueList []core.KafkaEventPayload, kafkaError error) {
	result, err_consume := kafka.Consume(topic, consumerGroup, batch)
	if err_consume != nil {
		kafkaError = errors.New(err_consume.Error())
	} else {
		for _, msg := range result.Messages {
			var kafKeyValue core.KafkaEventPayload
			var valueData core.KafkaValue_CBtoKafka_Struct

			valueErr := json.Unmarshal(msg.Value, &valueData)
			if valueErr != nil {
				kafkaError = logging.EnrichErrorWithStackTrace(valueErr)
			}
			kafKeyValue.Payload = valueData.Key
			kafKeyValue.Event = valueData.Event
			KafkaKeyValueList = append(KafkaKeyValueList, kafKeyValue)
		}
	}
	return KafkaKeyValueList, kafkaError
}

// based on the latest payload event
// pay load means key
func removeDuplicate(KafkaKeyValueSlice []core.KafkaEventPayload) map[string]string {
	KafkaKeyValueMap := make(map[string]string)

	for _, item := range KafkaKeyValueSlice {
		KafkaKeyValueMap[item.Payload] = item.Event
	}
	return KafkaKeyValueMap
}

// keep & skip couchbase document based on the configuration
func filterMessages(KafkaMessages map[string]string) map[string]string {
	KafkaKeyValue := make(map[string]string)

	if keepDockeyPrefix != "" {
		keepKeyPrefix := strings.Split(keepDockeyPrefix, ",")
		logging.DoLoggingLevelBasedLogs(logging.Debug, "Keep: "+keepDockeyPrefix+"   keepkeyprefix length: "+strconv.Itoa(len(keepKeyPrefix)), nil)
		if len(keepKeyPrefix) > 0 {
			for docKey, docEvent := range KafkaMessages {
				if filterDocKey(keepKeyPrefix, docKey) {
					KafkaKeyValue[docKey] = docEvent
				}
			}
		}
	} else if skipDockeyPrefix != "" {
		skipKeyPrefix := strings.Split(skipDockeyPrefix, ",")
		logging.DoLoggingLevelBasedLogs(logging.Debug, "skip: "+skipDockeyPrefix+"   skipkeyprefix length: "+strconv.Itoa(len(skipKeyPrefix)), nil)
		if len(skipKeyPrefix) > 0 {
			for docKey, docEvent := range KafkaMessages {
				if !filterDocKey(skipKeyPrefix, docKey) {
					KafkaKeyValue[docKey] = docEvent
				}
			}
		}
	} else {
		logging.DoLoggingLevelBasedLogs(logging.Debug, "going into else part: keepprefix or skipprefix not exists", nil)
		KafkaKeyValue = KafkaMessages
	}
	return KafkaKeyValue
}

func filterDocKey(prefixs []string, docKey string) (flag bool) {
	for _, prefix := range prefixs {
		if strings.HasPrefix(strings.ToLower(docKey), strings.ToLower(prefix)) {
			flag = true
			break
		}
	}
	return
}

// dividing the documents id or key as per no of receivers
func makeMessageBatch(documentKeys map[string]string) (KeyReceiverList []core.ReceiverMetaData) {
	j := 0
	for k, v := range documentKeys {
		KeyReceiverList = append(KeyReceiverList, core.ReceiverMetaData{
			Payload:      k,
			ClientObject: receiverGlobalConnections[j],
			Event:        v,
		})
		j++
		if j == len(receiverDomainsArray) {
			j = 0
		}
	}
	return KeyReceiverList
}

// get the document from couchbase and send to the receiver
func getAndSendDocument(receiver core.ReceiverMetaData, rl chan core.RoutinesResult) {
	var response core.RoutinesResult
	var flag bool //for the couchbase document event
	var cbend int64

	logging.DoLoggingLevelBasedLogs(logging.Debug, "cb details: bucket name: "+sourceBucket+" key: "+receiver.Payload+" event: "+receiver.Event, nil)

	if receiver.Event == "mutation" {
		flag = true
		// starting document get time
		cbstart := time.Now()

		cbResult, cbError := getDocFromCouchbase(sourceBucket, receiver.Payload)

		cbend = time.Since(cbstart).Milliseconds()

		if cbError != nil {
			if !errors.Is(cbError, gocb.ErrDocumentNotFound) {
				response = errorResponse(cbError.Error(), cbError)
			}
		} else {
			// end document get time

			cbResponse := core.DocumentMetaData{
				Document: cbResult,
				Key:      receiver.Payload,
				Bucket:   destinationBucket,
			}
			// starting time for send document
			updateTime := time.Now()

			srError := sendToReceiver(receiver.ClientObject, cbResponse, upsertEndpoint)
			response.RtUpdateTotal = time.Since(updateTime).Milliseconds()
			if srError != nil {
				response = errorResponse(srError.Error(), srError)
			} else {
				response = errorResponse("", nil)
			}
		}
	} else {
		flag = false

		cbResponse := core.DocumentMetaData{
			Document: couchbase.CBRawDataResult{},
			Key:      receiver.Payload,
			Bucket:   destinationBucket,
		}

		// send remove document time
		rtDeleteTotalStart := time.Now()
		srError := sendToReceiver(receiver.ClientObject, cbResponse, removeEndpoint)
		response.RtDeleteTotal = time.Since(rtDeleteTotalStart).Milliseconds()

		if srError != nil {
			response = errorResponse(srError.Error(), srError)
		} else {
			response = errorResponse("", nil)
		}
	}

	response.FlagEvent = flag
	response.RtCbTotal = cbend
	rl <- response
}

// get document from couchbase
func getDocFromCouchbase(bucket string, key string) (cbresult couchbase.CBRawDataResult, couchbaseError error) {
	_, err_get := couchbase.Raw_GetDocument(bucket, key, &cbresult)
	if err_get != nil {
		if errors.Is(err_get, gocb.ErrDocumentNotFound) {
			couchbaseError = err_get
			logging.DoLoggingLevelBasedLogs(logging.Debug, "Document Not Found: "+key, nil)
		} else {
			couchbaseError = logging.EnrichErrorWithStackTrace(err_get)
		}
	} else {
		couchbaseError = nil
	}
	return cbresult, couchbaseError
}

// return the errorMessage and errorStatus struct
func errorResponse(ErrorMessage string, errOut error) core.RoutinesResult {
	var rr core.RoutinesResult
	rr.Message = ErrorMessage
	rr.Error = errOut
	return rr
}

func produceMetrics(metrics core.MetricsObservation) (errout error) {

	metrics.CountBatch = batch
	metrics.Type = typeName
	metrics.Topic = topic

	metricsJsonData, metricsErr := json.Marshal(&metrics)
	if metricsErr != nil {
		errout = logging.EnrichErrorWithStackTrace(metricsErr)
	} else {
		logging.DoLoggingLevelBasedLogs(logging.Debug, "metrics log: "+string(metricsJsonData), nil)

		_, err_produce := kafka.Produce(metricsTopic, kafkaMessageKey, metrics)
		if err_produce != nil {
			errout = logging.EnrichErrorWithStackTrace(err_produce)
		} else {
			errout = nil
		}
		logging.DoLoggingLevelBasedLogs(logging.Debug, "successfully produce metrics on kafka", nil)
	}
	return
}
