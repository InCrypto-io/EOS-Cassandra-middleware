package routes

import (
	"net/http"
)


func (r *Router) handleGetActions() http.HandlerFunc {
	type RequestParams struct {
		AccountName string `json:"account_name"`
		Pos         *int64 `json:"pos"`
		Offset      *int64 `json:"offset"`
		//TODO: additional params
	}

	type Action struct {
		GlobalActionSeq interface{} `json:"global_action_seq"`
		// TODO...
	}
	type Response struct {
		Actions               []Action `json:"actions"`
		LastIrreversibleBlock   uint64 `json:"last_irreversible_block"`
	}

	return func(writer http.ResponseWriter, request *http.Request) {
		// TODO: implement
	}
}