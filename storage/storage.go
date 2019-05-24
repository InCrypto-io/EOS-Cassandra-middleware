package storage

import "encoding/json"

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
	AccountActionSeq          uint64 `json:"account_action_seq"`
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


type IHistoryStorage interface {
	GetActions(GetActionArgs)                        (GetActionsResult,            error)
	GetTransaction(GetTransactionArgs)               (GetTransactionResult,        error)
	GetKeyAccounts(GetKeyAccountsArgs)               (GetKeyAccountsResult,        error)
	GetControlledAccounts(GetControlledAccountsArgs) (GetControlledAccountsResult, error)
}