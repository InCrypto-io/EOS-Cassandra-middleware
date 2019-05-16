package storage

import (
	"errors"
)


type MockedCassandraHistoryStorage struct {
	IHistoryStorage

	mockedLastIrreversibleBlock uint64
}


func NewMockedCassandraHistoryStorage(lib uint64) *MockedCassandraHistoryStorage {
	return &MockedCassandraHistoryStorage{ mockedLastIrreversibleBlock: lib}
}


func (mchs *MockedCassandraHistoryStorage) GetActions(args GetActionArgs) (GetActionsResult, error) {
	result := GetActionsResult{ LastIrreversibleBlock: mchs.mockedLastIrreversibleBlock, Actions: make([]Action, 0) }

	if args.AccountName == "" {
		return result, nil
	}

	result.Actions = append(result.Actions, Action{ GlobalActionSeq: 1337 })
	return result, nil
}

func (mchs *MockedCassandraHistoryStorage) GetTransaction(args GetTransactionArgs) (GetTransactionResult, error) {
	result := GetTransactionResult{ LastIrreversibleBlock: mchs.mockedLastIrreversibleBlock }

	if args.ID == "" {
		return result, errors.New("Invalid transaction ID: " + args.ID)
	}

	result.ID = args.ID
	return result, nil
}

func (mchs *MockedCassandraHistoryStorage) GetKeyAccounts(args GetKeyAccountsArgs) (GetKeyAccountsResult, error) {
	result := GetKeyAccountsResult{ AccountNames: make([]string, 0) }

	if args.PublicKey == "" {
		return result, nil
	}

	result.AccountNames = append(result.AccountNames, "1", "2", "3")
	return result, nil
}

func (mchs *MockedCassandraHistoryStorage) GetControlledAccounts(args GetControlledAccountsArgs) (GetControlledAccountsResult, error) {
	result := GetControlledAccountsResult{ ControlledAccounts: make([]string, 0) }

	if args.ControllingAccount == "" {
		return result, nil
	}

	result.ControlledAccounts = append(result.ControlledAccounts, "4", "5", "6")
	return result, nil
}