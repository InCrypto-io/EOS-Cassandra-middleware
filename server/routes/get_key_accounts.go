package routes

import (
	"net/http"
)


func (r *Router) handleGetKeyAccounts() http.HandlerFunc {
	type RequestParams struct {
		PublicKey string `json:"public_key"`
	}

	type Response struct {
		AccountNames []string `json:"account_names"`
	}

	return func(writer http.ResponseWriter, request *http.Request) {
		// TODO: implement
	}
}