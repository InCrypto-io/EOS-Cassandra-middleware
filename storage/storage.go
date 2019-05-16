package storage

//get_actions
type GetActionArgs struct {
	AccountName string `json:"account_name"`
	Pos         *int64 `json:"pos"`
	Offset      *int64 `json:"offset"`
	//TODO: additional params
}

type Action struct {
	GlobalActionSeq interface{} `json:"global_action_seq"`
	// TODO...
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
	ID                      string `json:"id"`
	// TODO...
	LastIrreversibleBlock   uint64 `json:"last_irreversible_block"`
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