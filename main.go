package main

import (
	"datareplication_sender/config"
	"datareplication_sender/core"
	"datareplication_sender/replication"
	"datareplication_sender/server"
	"datareplication_sender/storage/couchbase"
	"datareplication_sender/storage/logging"
	"strconv"
	"time"

	"github.com/go-co-op/gocron"
)

var (
	serverPort                   = config.GetString("HttpServer.Port")
	initialDelayInSeconds        = config.GetInt64("InitialDelayInSeconds")
	evaluateSenderActiveStatusAt = config.GetString("EvaluateSenderActiveStatusAt")
	sourceBucket                 = config.GetString("Couchbase.Buckets.SourceBucket")
)

func main() {
	// assign the intial value
	core.LastActiveTime = time.Now().UTC()

	logging.DoLoggingLevelBasedLogs(logging.Info, "Wait for "+strconv.FormatInt(initialDelayInSeconds, 10)+" Seconds", nil)
	time.Sleep(time.Duration(initialDelayInSeconds) * time.Second)

	_, err := couchbase.InitializeConnectionBeforeUse(sourceBucket)
	if err != nil {
		logging.DoLoggingLevelBasedLogs(logging.Error, "", logging.EnrichErrorWithStackTrace(err))
	} else {

		// initialize HTTP Clients for Receivers
		errInit := replication.InitializeReceiverClients()

		// all the receiver status is active then processBatch
		if len(errInit) <= 0 {
			logging.DoLoggingLevelBasedLogs(logging.Debug, "all receiver connected successfully.", nil)

			// check whether to activate sender or not - first time check
			replication.DecideSenderActiveStatus()

			// check whether to activate sender or not - keep checking at defined interval
			s := gocron.NewScheduler(time.UTC)
			s.Cron(evaluateSenderActiveStatusAt).Do(replication.DecideSenderActiveStatus)
			s.StartAsync()

			// start cbtocb replication process
			go replication.StartReplication()

		} else {
			core.SenderStatus = core.InActiveReceivers
			for _, err_msg := range errInit {
				logging.DoLoggingLevelBasedLogs(logging.Error, "", err_msg)
			}
		}
	}

	// register Server
	server.RegisterServer()
	logging.DoLoggingLevelBasedLogs(logging.Info, "starting sender server at port: "+serverPort, nil)
}
