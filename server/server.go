package server

import (
	"datareplication_sender/config"
	"datareplication_sender/healthcheck"
	"datareplication_sender/storage/logging"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
)

var port = config.GetInt("HttpServer.Port")

func RegisterServer() {

	s := chi.NewRouter()

	s.Get("/statichealthcheck", staticHealthCheck())
	s.Get("/healthcheck", healthcheck.HealthCheck())

	httpPort := ":" + strconv.Itoa(port)
	err_conn := http.ListenAndServe(httpPort, s)
	if err_conn != nil {
		logging.DoLoggingLevelBasedLogs(logging.Error, "", logging.EnrichErrorWithStackTrace(errors.New("http connection error:"+err_conn.Error())))
	}
}

func staticHealthCheck() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logging.DoLoggingLevelBasedLogs(logging.Debug, "healthcheck success", nil)
		w.Write([]byte("Helthcheck Success"))
	}
}
