package routes

import (
	"EOS-Cassandra-middleware/storage"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)


func (r *Router) handleGetActions() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		bytes, err := ioutil.ReadAll(request.Body)
		defer request.Body.Close()
		if err != nil {
			writeErrorResponse(writer, http.StatusInternalServerError, "Internal service error")
			log.Println("Failed to read request body. Error: " + err.Error())
			return
		}

		var args storage.GetActionArgs
		if len(bytes) > 0 {
			if err = json.Unmarshal(bytes, &args); err != nil {
				if _, ok := err.(*json.SyntaxError); ok {
					writeErrorResponse(writer, http.StatusBadRequest, "Invalid json in request body")
					log.Println("Invalid request body. Error: " + err.Error())
					return
				}
			}
		}

		response, err := r.historyStorage.GetActions(args)
		if err != nil {
			writeErrorResponse(writer, http.StatusInternalServerError, "Internal service error")
			log.Println("Got error from IHistoryStorage.GetActions(). Error: " + err.Error())
			return
		}
		b, err := json.Marshal(response)
		if err != nil {
			writeErrorResponse(writer, http.StatusInternalServerError, "Internal service error")
			log.Println("Failed to marshal response. Error: " + err.Error())
			return
		}
		fmt.Fprintf(writer, string(b))
	}
}