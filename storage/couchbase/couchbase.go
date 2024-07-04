package couchbase

import (
	"datareplication_sender/config"
	"errors"
	"log"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v9"
)

var endpoint = config.GetString("Couchbase.Host")
var username = config.GetString("Couchbase.Username")
var password = config.GetString("Couchbase.Password")
var cbBucketCollections = make(map[string](*gocb.Collection))
var cbRetriveTimeoutSecond = config.GetInt("Couchbase.RetriveTimeoutSecond")

func init() { log.SetFlags(log.Lshortfile | log.LstdFlags) }

// Initialize the couchbase connection - before use
func InitializeConnectionBeforeUse(bucket string) (*gocb.Collection, error) {
	return initializeCBConnection(bucket)
}

// Initialize the couchbase connection
func initializeCBConnection(bucket string) (*gocb.Collection, error) {

	if bucket != "" {
		opts := gocb.ClusterOptions{
			Username: username,
			Password: password,
		}
		//Initialize the connection
		cluster, err_connect := gocb.Connect(endpoint, opts)
		if err_connect != nil {
			return nil, err_connect
		} else {
			bucketConn := cluster.Bucket(bucket)
			err_ready := bucketConn.WaitUntilReady(50*time.Second, nil)
			if err_ready != nil {
				log.Println("CB:Error while connecting with cocuhbase")
				return nil, err_ready
			} else {
				collection := bucketConn.DefaultCollection()
				// Add into global collection map
				cbBucketCollections[bucket] = collection
				return collection, nil
			}
		}
	} else {
		return nil, errors.New("CB:Bucket name can not be blank or empty")
	}
}

func getCollection(bucket string) (*gocb.Collection, error) {
	if cbBucketCollections[bucket] == nil {
		return initializeCBConnection(bucket)
	} else {
		return cbBucketCollections[bucket], nil
	}
}

func GetDocument(bucket string, docKey string, objPtr interface{}) (cas uint64, errOut error) {
	if bucket != "" && docKey != "" {
		collection, err_collection := getCollection(bucket)

		if err_collection != nil {
			return 0, err_collection
		} else {
			result, err := collection.Get(docKey, &gocb.GetOptions{})
			if err != nil {
				log.Println("CB:Error in Get method: " + err.Error())
				return 0, err
			} else {
				result.Content(objPtr)
				return uint64(result.Cas()), nil
			}
		}
	} else {
		return 0, errors.New("CB:Bucket name and doc key can not be blank or empty")
	}
}

func AddOrUpdateDocument(bucket string, docKey string, data interface{}) (errOut error) {
	if bucket != "" && docKey != "" {
		collection, err_collection := getCollection(bucket)

		if err_collection != nil {
			return err_collection
		} else {
			_, err := collection.Upsert(docKey, data, &gocb.UpsertOptions{})
			if err != nil {
				log.Println("CB:Error in Upsert method: " + err.Error())
			}
			return err
		}
	} else {
		return errors.New("CB:Bucket name and doc key can not be blank or empty")
	}
}

func ReplaceDocument(bucket string, docKey string, data interface{}, cas uint64) (errOut error) {
	if bucket != "" && docKey != "" {
		collection, err_collection := getCollection(bucket)

		if err_collection != nil {
			return err_collection
		} else {
			// Replace Document with cas
			_, err := collection.Replace(docKey, data, &gocb.ReplaceOptions{Cas: gocb.Cas(cas)})
			if err != nil {
				log.Println("CB:Error in Replace method: " + err.Error())
			}
			return err
		}
	} else {
		return errors.New("CB:Bucket name and doc key can not be blank or empty")
	}
}

func RemoveDocument(bucket string, docKey string) (errOut error) {
	if bucket != "" && docKey != "" {
		collection, err_collection := getCollection(bucket)

		if err_collection != nil {
			return err_collection
		} else {
			_, err := collection.Remove(docKey, &gocb.RemoveOptions{})
			if err != nil {
				log.Println("CB:Error in Remove method: " + err.Error())
			}
			return err
		}
	} else {
		return errors.New("CB:Bucket name and doc key can not be blank or empty")
	}
}

// RawDataTranscoder
// This will be used in data transfer application where we need to transfer exact data with exact type to destination
// session documents generated from node.js was in the string type (although it is looking like json only) so we need to transport it as raw string type
type RawDataTranscoder struct {
}

type CBValueType int

const (
	Json CBValueType = iota
	String
	Binary
)

type CBRawDataResult struct {
	Type  CBValueType
	Value []byte
}

// NewRawDataTranscoder returns a new RawDataTranscoder.
func NewRawDataTranscoder() *RawDataTranscoder {
	return &RawDataTranscoder{}
}

// Decode applies raw data transcoding behaviour to decode into a Go type.
func (t *RawDataTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return errors.New("CB:Unexpected value compression")
	}

	var result CBRawDataResult

	// Normal types of decoding
	if valueType == gocbcore.BinaryType {
		result.Type = Binary
	} else if valueType == gocbcore.StringType {
		result.Type = String
	} else if valueType == gocbcore.JSONType {
		result.Type = Json
	}

	result.Value = bytes

	switch v := out.(type) {
	case *CBRawDataResult:
		*v = result
		return nil
	default:
		return errors.New("CB:Out parameter should be of type CBRawDataResult")
	}
}

// Encode applies raw data transcoding behavior to encode a Go type.
func (t *RawDataTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32

	switch typeValue := value.(type) {
	case string:
		bytes = []byte(typeValue)
		flags = gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression)
	case []byte:
		bytes = typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression)
	default:
		return nil, 0, errors.New("CB:Only binary and string data is supported by RawDataTranscoder")
	}

	return bytes, flags, nil
}

func Raw_GetDocument(bucket string, docKey string, objPtr interface{}) (cas uint64, errOut error) {

	if bucket != "" && docKey != "" {

		collection, err_collection := getCollection(bucket)

		if err_collection != nil {
			return 0, err_collection
		} else {
			// Get Document
			transcoder := NewRawDataTranscoder()
			result, err := collection.Get(docKey, &gocb.GetOptions{
				Transcoder: transcoder,
				Timeout: time.Duration(cbRetriveTimeoutSecond) * time.Second,
			})
			if err != nil {
				log.Println("CB:Error in Get method: " + err.Error())
				return 0, err
			} else {
				// Get Document
				result.Content(objPtr)
				return uint64(result.Cas()), nil
			}
		}
	} else {
		return 0, errors.New("bucektname and dockey can not be blank or empty in GetDocument method")
	}
}

func Raw_AddOrUpdateDocument(bucket string, docKey string, data interface{}) (errOut error) {
	if bucket != "" && docKey != "" {
		collection, err_collection := getCollection(bucket)

		if err_collection != nil {
			return err_collection
		} else {
			transcoder := NewRawDataTranscoder()
			_, err := collection.Upsert(docKey, data, &gocb.UpsertOptions{
				Transcoder: transcoder,
				Timeout: time.Duration(cbRetriveTimeoutSecond) * time.Second,
			})
			if err != nil {
				log.Println("CB:Error in Upsert method: " + err.Error())
			}
			return err
		}
	} else {
		return errors.New("CB:Bucket name and doc key can not be blank or empty")
	}
}
