package mocked_storage

import (
	"EOS-Cassandra-middleware/storage"
	"errors"
)


type MockedStorage struct {
	storage.IHistoryStorage

	mockedLastIrreversibleBlock uint64
}


func NewMockedStorage(lib uint64) *MockedStorage {
	return &MockedStorage{ mockedLastIrreversibleBlock: lib}
}


func (ms *MockedStorage) GetActions(args storage.GetActionArgs) (storage.GetActionsResult, error) {
	result := storage.GetActionsResult{ LastIrreversibleBlock: ms.mockedLastIrreversibleBlock, Actions: make([]storage.Action, 0) }

	if args.AccountName == "" {
		return result, nil
	}

	result.Actions = append(result.Actions, storage.Action{ GlobalActionSeq: 1337 })
	return result, nil
}

func (ms *MockedStorage) GetTransaction(args storage.GetTransactionArgs) (storage.GetTransactionResult, error) {
	result := storage.GetTransactionResult{ LastIrreversibleBlock: ms.mockedLastIrreversibleBlock }

	if args.ID == "" {
		return result, errors.New("Invalid transaction ID: " + args.ID)
	}

	result.ID = args.ID
	return result, nil
}

func (ms *MockedStorage) GetKeyAccounts(args storage.GetKeyAccountsArgs) (storage.GetKeyAccountsResult, error) {
	result := storage.GetKeyAccountsResult{ AccountNames: make([]string, 0) }

	if args.PublicKey == "" {
		return result, nil
	}

	result.AccountNames = append(result.AccountNames, "1", "2", "3")
	return result, nil
}

func (ms *MockedStorage) GetControlledAccounts(args storage.GetControlledAccountsArgs) (storage.GetControlledAccountsResult, error) {
	result := storage.GetControlledAccountsResult{ ControlledAccounts: make([]string, 0) }

	if args.ControllingAccount == "" {
		return result, nil
	}

	result.ControlledAccounts = append(result.ControlledAccounts, "4", "5", "6")
	return result, nil
}