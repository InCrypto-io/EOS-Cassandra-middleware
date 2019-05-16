package routes

import (
	"EOS-Cassandra-middleware/storage"
	"encoding/json"
	"net/http"
)

const ApiPath string = "/v1/history/"


type ErrorResult struct {
	Code       int `json:"code"`
	Message string `json:"message"`
}


func writeErrorResponse(writer http.ResponseWriter, status int, message string) {
	writer.WriteHeader(status)
	response := ErrorResult { Code: status, Message: message }
	json.NewEncoder(writer).Encode(response)
}

func onlyGetOrPost(h http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodPost {
			writeErrorResponse(writer, http.StatusMethodNotAllowed, "Invalid request method.")
			return
		}
		h(writer, request)
	}
}


type Router struct {
	http.ServeMux

	historyStorage storage.IHistoryStorage
}


func NewRouter(hs storage.IHistoryStorage) *Router {

	router := Router{ historyStorage: hs }

	handler := http.NewServeMux()
	handler.HandleFunc(ApiPath + "get_actions", onlyGetOrPost(router.handleGetActions()))
	handler.HandleFunc(ApiPath + "get_transaction", onlyGetOrPost(router.handleGetTransaction()))
	handler.HandleFunc(ApiPath + "get_key_accounts", onlyGetOrPost(router.handleGetKeyAccounts()))
	handler.HandleFunc(ApiPath + "get_controlled_accounts", onlyGetOrPost(router.handleGetControlledAccounts()))
	router.ServeMux = *handler

	return &router
}