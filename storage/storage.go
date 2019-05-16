package storage


type IHistoryStorage interface {
	GetActions(/*TODO: arguments*/) /*TODO: response*/
	GetTransaction(/*TODO: arguments*/) /*TODO: response*/
	GetKeyAccounts(/*TODO: arguments*/) /*TODO: response*/
	GetControlledAccounts(/*TODO: arguments*/) /*TODO: response*/
}