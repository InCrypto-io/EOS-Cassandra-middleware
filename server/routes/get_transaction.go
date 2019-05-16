package routes

import (
	"net/http"
)


func (r *Router) handleGetTransaction() http.HandlerFunc {
	type RequestParams struct {
		ID string `json:"id"`
	}

	type Response struct {
		ID string `json:"id"`
		// TODO...
	}

	return func(writer http.ResponseWriter, request *http.Request) {
		// TODO: implement
	}
}