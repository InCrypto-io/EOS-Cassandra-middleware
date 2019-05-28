package storage

import (
	"EOS-Cassandra-middleware/error_result"
	"encoding/json"
	"time"
)

//get_actions
type GetActionArgs struct {
	AccountName string `json:"account_name"`
	Pos         *int64 `json:"pos"`
	Offset      *int64 `json:"offset"`
}

// Normalize() initializes Pos and Offset with default values if not set
// returns position in account history, count of actions to get and order of history (true=asc, false=desc)
func (args *GetActionArgs) Normalize() (int64, int64, bool) {
	if args.Pos == nil {
		args.Pos = new(int64)
		*args.Pos = -1
	}
	if args.Offset == nil {
		args.Offset = new(int64)
		*args.Offset = -20
	}
	order := true
	pos := *args.Pos
	count := *args.Offset
	if pos == -1 {
		order = false
		if count >= 0 {
			pos -= count
			count += 1
		} else {
			count = -(count - 1)
		}
	} else {
		if count >= 0 {
			count += 1
		} else {
			pos += count
			count = -(count - 1)
		}
	}
	if pos + count <= 0 {
		return 0, 0, order
	} else if pos < 0 {
		count += pos
		pos = 0
	}
	return pos, count, order
}


type Action struct {
	GlobalActionSeq      interface{} `json:"global_action_seq"`
	AccountActionSeq         *uint64 `json:"account_action_seq,omitempty"`
	BlockNum             interface{} `json:"block_num"`
	BlockTime            interface{} `json:"block_time"`
	ActionTrace      json.RawMessage `json:"action_trace"`
}

type GetActionsResult struct {
	Actions               []Action `json:"actions"`
	LastIrreversibleBlock   uint64 `json:"last_irreversible_block"`
}

//get_transaction
type GetTransactionArgs struct {
	ID string `json:"id"`
}

type GetTransactionResult struct {
	ID                                    string `json:"id"`
	Trx                   map[string]interface{} `json:"trx"`
	BlockTime                        interface{} `json:"block_time"`
	BlockNum                         interface{} `json:"block_num"`
	Traces                         []interface{} `json:"traces"`
	LastIrreversibleBlock                 uint64 `json:"last_irreversible_block"`
}

//get_key_accounts
type GetKeyAccountsArgs struct {
	PublicKey string `json:"public_key"`
}

type GetKeyAccountsResult struct {
	AccountNames []string `json:"account_names"`
}

//get_controlled_accounts
type GetControlledAccountsArgs struct {
	ControllingAccount string `json:"controlling_account"`
}

type GetControlledAccountsResult struct {
	ControlledAccounts []string `json:"controlled_accounts"`
}

//find_actions
type FindActionsArgs struct {
	AccountName      string `json:"account_name"`
	FromDate    interface{} `json:"from_date"`
	ToDate      interface{} `json:"to_date"`
	LastDays        *uint32 `json:"last_days"`
	Data             string `json:"data"`
}

func (args *FindActionsArgs) GetFromTime() *time.Time {
	var t *time.Time
	if s, ok := args.FromDate.(string); ok {
		tmp, err := time.Parse("2006-01-02T15:04:05", s)
		if err != nil {
			return t
		}
		t = &tmp
	} else if n, ok := args.FromDate.(float64); ok {
		unixT := time.Unix(int64(n), 0)
		t = &unixT
	}
	return t
}

func (args *FindActionsArgs) GetToTime() *time.Time {
	var t *time.Time
	if s, ok := args.ToDate.(string); ok {
		tmp, err := time.Parse("2006-01-02T15:04:05", s)
		if err != nil {
			return t
		}
		t = &tmp
	} else if n, ok := args.ToDate.(float64); ok {
		unixT := time.Unix(int64(n), 0)
		t = &unixT
	}
	return t
}

type FindActionsResult struct {
	Actions               []Action `json:"actions"`
	LastIrreversibleBlock   uint64 `json:"last_irreversible_block"`
}


type IHistoryStorage interface {
	GetActions(GetActionArgs)                        (GetActionsResult,            *error_result.ErrorResult)
	GetTransaction(GetTransactionArgs)               (GetTransactionResult,        *error_result.ErrorResult)
	GetKeyAccounts(GetKeyAccountsArgs)               (GetKeyAccountsResult,        *error_result.ErrorResult)
	GetControlledAccounts(GetControlledAccountsArgs) (GetControlledAccountsResult, *error_result.ErrorResult)
	FindActions(FindActionsArgs)                     (FindActionsResult,           *error_result.ErrorResult)
}