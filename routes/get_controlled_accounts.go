package routes

import (
	"net/http"
)


func (r *Router) handleGetControlledAccounts() http.HandlerFunc {
	type RequestParams struct {
		ControllingAccount string `json:"controlling_account"`
	}

	type Response struct {
		ControlledAccounts []string `json:"controlled_accounts"`
	}

	return func(writer http.ResponseWriter, request *http.Request) {
		// TODO: implement
	}
}