package logging

import (
	"datareplication_sender/config"
	"errors"
	"log"
	"runtime/debug"
	"strings"
)

var configuredLogLevel = config.GetString("LogLevel")

type LogLevel int32

const (
	Debug LogLevel = iota + 1
	Info
	Warn
	Error
)

/*
*

	Return the LogLavel field as string

*
*/
func (l LogLevel) String() string {
	return [...]string{"DEBUG", "INFO", "WARN", "ERROR"}[l-1]
}

/*
*

	Parameter:
	1) Logging level as a LogLevel,
	2) Log Message as a string
	Print the log message as per LoggingLevel and Config LogginLevel

*
*/
func DoLoggingLevelBasedLogs(level LogLevel, message string, errorMessage error) {

	configuredLogLevel = strings.ToUpper(configuredLogLevel)

	if level == Error {
		printLogsMessage(level.String(), message, errorMessage)
	} else if level == Warn && (configuredLogLevel == Warn.String() || configuredLogLevel == Info.String() || configuredLogLevel == Debug.String()) {
		printLogsMessage(level.String(), message, errorMessage)
	} else if level == Info && (configuredLogLevel == Info.String() || configuredLogLevel == Debug.String()) {
		printLogsMessage(level.String(), message, errorMessage)
	} else if level == Debug && configuredLogLevel == Debug.String() {
		printLogsMessage(level.String(), message, errorMessage)
	}
}

func printLogsMessage(level string, message string, errorMessage error) {
	if errorMessage != nil {
		log.Println(errorMessage)
	} else {
		log.Println(level + ": " + message)
	}
}

func EnrichErrorWithStackTrace(msg error) (errOut error) {
	err := []byte(msg.Error() + "\n")
	stackTrace := debug.Stack()
	errorMessage := removeUnnecerrayLogFromStackTrace(stackTrace, 1, 2)
	apeendErr := append(err, errorMessage...)
	errOut = errors.New(string(apeendErr))
	return
}

func EnrichErrorWithStackTraceAndLog(msg error) {
	log.Println(EnrichErrorWithStackTrace(msg))
}

func removeUnnecerrayLogFromStackTrace(errorSlice []byte, index int, noOfLineRemove int) (newErrorSlice string) {

	stackTracedataArray := strings.Split(strings.Replace(string(errorSlice), "\r\n", "\n", -1), "\n")

	copy(stackTracedataArray[index:], stackTracedataArray[index+noOfLineRemove:])
	stackTracedataArray[len(stackTracedataArray)-noOfLineRemove] = ""
	stackTracedataArray = stackTracedataArray[:len(stackTracedataArray)-noOfLineRemove]

	newErrorSlice = strings.Join(stackTracedataArray, "\n")
	return
}
