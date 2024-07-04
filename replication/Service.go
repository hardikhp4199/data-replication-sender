package replication

import (
	"datareplication_sender/config"
	"datareplication_sender/core"
	"datareplication_sender/storage/logging"
	"datareplication_sender/util/common"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

var (
	receiverAuthUsername       = config.GetString("Receiver.Authorization.Username")
	receiverAuthPassword       = config.GetString("Receiver.Authorization.Password")
	healthcheckEndpoint        = config.GetString("Receiver.Endpoints.Healthcheck")
	cbtocbconfigrationEndpoint = config.GetString("Receiver.Endpoints.Cbtocbconfigration")
)

// return the errorstatus as a bool value and errormessage as a string
func sendToReceiver(clientObj core.Receiver, cbResponse core.DocumentMetaData, endPoint string) (errOut error) {

	json_data, err := json.Marshal(cbResponse)

	if err != nil {
		errOut = logging.EnrichErrorWithStackTrace(err)
	} else {
		//compress the data using gzip
		bodyData, err := common.CompressDataTOGzip(json_data)
		if err != nil {
			errOut = err
		} else {

			receiverURL := clientObj.Host + endPoint
			request, httperr := http.NewRequest("POST", receiverURL, &bodyData)
			request.SetBasicAuth(receiverAuthUsername, receiverAuthPassword)

			request.Header.Add("Content-Encoding", "gzip")
			request.Header.Add("Content-Type", "application/json")

			if httperr != nil {
				errOut = httperr
			} else {
				response, res_error := clientObj.Client.Do(request)
				if res_error != nil {
					errOut = logging.EnrichErrorWithStackTrace(res_error)
				} else if response.StatusCode == http.StatusUnauthorized {
					errOut = logging.EnrichErrorWithStackTrace(errors.New("receiver api (" + receiverURL + ") didn't respond with ok status. Returened status is: " + response.Status))
				} else {

					defer response.Body.Close()

					var res core.ResponseResult
					decodeError := json.NewDecoder(response.Body).Decode(&res)

					if decodeError != nil {
						errOut = logging.EnrichErrorWithStackTrace(decodeError)
					} else if res.Status == core.Error {
						errOut = logging.EnrichErrorWithStackTrace(errors.New(res.ErrorMessage))
					} else {
						errOut = nil
					}
				}
			}
		}
	}
	return
}

func getDestinationSenderConfig(receiverURL string, clientObj *http.Client) (cbConfigResponse core.CBConfigDetails, errOut error) {
	receiverURL = receiverURL + cbtocbconfigrationEndpoint

	request, httperr := http.NewRequest("POST", receiverURL, nil)
	request.SetBasicAuth(receiverAuthUsername, receiverAuthPassword)

	request.Header.Add("Content-Encoding", "gzip")
	request.Header.Add("Content-Type", "application/json")

	if httperr != nil {
		errOut = httperr
	} else {
		response, res_error := clientObj.Do(request)
		if res_error != nil {
			errOut = logging.EnrichErrorWithStackTrace(res_error)
		} else if response.StatusCode == http.StatusUnauthorized {
			errOut = logging.EnrichErrorWithStackTrace(errors.New("receiver api (" + receiverURL + ") didn't respond with ok status. Returened status is: " + response.Status))
		} else {
			defer response.Body.Close()
			var res core.ResponseResult

			decodeError := json.NewDecoder(response.Body).Decode(&res)

			if decodeError != nil {
				errOut = logging.EnrichErrorWithStackTrace(decodeError)
			} else if res.Status == core.Error {
				errOut = logging.EnrichErrorWithStackTrace(errors.New(res.ErrorMessage))
			} else {
				jsonUnmarshalErr := json.Unmarshal([]byte(res.SuccessMessage), &cbConfigResponse)
				if jsonUnmarshalErr != nil {
					errOut = logging.EnrichErrorWithStackTrace(jsonUnmarshalErr)
				}
			}
		}
	}
	return
}

func healthCheck(receiver core.Receiver) (healthChaekError error) {
	healthCheckURL := receiver.Host + healthcheckEndpoint

	request, re_err := http.NewRequest("GET", healthCheckURL, nil)

	if re_err != nil {
		healthChaekError = logging.EnrichErrorWithStackTrace(re_err)
	} else {
		request.SetBasicAuth(receiverAuthUsername, receiverAuthPassword)
		request.Header.Add("Content-Encoding", "gzip")
		request.Header.Add("Content-Type", "application/json")

		response, err := receiver.Client.Do(request)
		if err != nil {
			healthChaekError = logging.EnrichErrorWithStackTrace(errors.New(healthCheckURL + "  Connection error" + ", " + err.Error()))
		} else if response.StatusCode == http.StatusUnauthorized {
			healthChaekError = logging.EnrichErrorWithStackTrace(errors.New("receiver api (" + healthCheckURL + ") didn't respond with ok status. Returened status is: " + response.Status))
		} else {
			defer response.Body.Close()

			_, read_err := io.ReadAll(response.Body)
			if read_err != nil {
				healthChaekError = logging.EnrichErrorWithStackTrace(read_err)
			}
		}
	}
	return healthChaekError
}
