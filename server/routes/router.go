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


func onlyGetOrPost(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			response := ErrorResult { Code: http.StatusMethodNotAllowed, Message: "Invalid request method." }
			json.NewEncoder(w).Encode(response)
			return
		}
		h(w, r)
	}
}


type Router struct {
	http.ServeMux

	historyStorage storage.IHistoryStorage
}


func NewRouter() *Router {
	handler := http.NewServeMux() //TODO: replace empty handlers with real ones
	handler.HandleFunc(ApiPath + "get_actions", onlyGetOrPost(func(writer http.ResponseWriter, request *http.Request) {}))
	handler.HandleFunc(ApiPath + "get_transaction", onlyGetOrPost(func(writer http.ResponseWriter, request *http.Request) {}))
	handler.HandleFunc(ApiPath + "get_key_accounts", onlyGetOrPost(func(writer http.ResponseWriter, request *http.Request) {}))
	handler.HandleFunc(ApiPath + "get_controlled_accounts", onlyGetOrPost(func(writer http.ResponseWriter, request *http.Request) {}))

	var hs storage.IHistoryStorage //TODO: init

	return &Router{ ServeMux: *handler, historyStorage: hs }
}