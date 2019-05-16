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