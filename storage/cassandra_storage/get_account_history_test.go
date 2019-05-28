package cassandra_storage

import (
	"EOS-Cassandra-middleware/storage"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)


func Test_getAccountHistory(t *testing.T) {
	hs, err := NewTestCassandraStorage()
	if err != nil {
		t.Error("Failed to create history storage object: " + err.Error())
		return
	}
	defer hs.Close()

	err = hs.Session.Query(fmt.Sprintf("CREATE TABLE %s (global_seq varint, parent varint, doc text, PRIMARY KEY(global_seq))", TableActionTrace)).Exec()
	if err != nil {
		t.Error("Failed to create table: " + err.Error())
		return
	}
	err = hs.Session.Query(fmt.Sprintf("CREATE TABLE %s (shard_id timestamp, account_name text, global_seq varint, block_time timestamp, parent varint," +
		" PRIMARY KEY((account_name, shard_id), block_time, global_seq))", TableAccountActionTrace)).Exec()
	if err != nil {
		t.Error("Failed to create table: " + err.Error())
		return
	}
	err = hs.Session.Query(fmt.Sprintf("CREATE TABLE %s (account_name text, shard_id timestamp, PRIMARY KEY(account_name, shard_id))", TableAccountActionTraceShard)).Exec()
	if err != nil {
		t.Error("Failed to create table: " + err.Error())
		return
	}

	testAccount := "test"
	fullTestAccountHistory := []storage.Action{}
	globalSeq := uint64(1)
	shardTime, err := time.Parse("2006-01-02T15:04:05", "2018-06-09T11:11:11")
	if err != nil {
		t.Error("Failed to create shard: " + err.Error())
		return
	}
	//emptyShard := Timestamp{ Time: shardTime }
	shardTime = shardTime.Add(time.Millisecond * 500)
	shard1 := Timestamp{ Time: shardTime }
	err = hs.Session.Query(fmt.Sprintf(TemplateInsertAccountActionTraceShard, testAccount, shard1.String())).Exec()
	if err != nil {
		t.Error("Insert failed: " + err.Error())
		return
	}
	blockTime := shard1
	for i := 0; i < 1000; i++ { //for 1000 minutes
		for j := 0; j < 10; j++ { //insert 10 records per minute
			err = hs.Session.Query(fmt.Sprintf(TemplateInsertAccountActionTrace, testAccount, shard1.String(), globalSeq, blockTime.String())).Exec()
			if err != nil {
				t.Error("Insert failed: " + err.Error())
				return
			}
			var doc ActionTraceDoc
			doc.Receipt = make(map[string]interface{})
			doc.Receipt["global_sequence"] = globalSeq
			b, err := json.Marshal(doc)
			if err != nil {
				t.Error("Marshal failed: " + err.Error())
				return
			}
			err = hs.Session.Query(fmt.Sprintf(TemplateInsertActionTrace, globalSeq, string(b))).Exec()
			if err != nil {
				t.Error("Insert failed: " + err.Error())
				return
			}

			accountActionSeq := globalSeq - 1
			action := storage.Action{ GlobalActionSeq:globalSeq, AccountActionSeq:&accountActionSeq }
			fullTestAccountHistory = append(fullTestAccountHistory, action)

			globalSeq += 1
		}
		blockTime.Time = blockTime.Add(time.Minute)
	}

	blockTime.Time = blockTime.Add(time.Hour)
	shard2 := blockTime
	err = hs.Session.Query(fmt.Sprintf(TemplateInsertAccountActionTraceShard, testAccount, shard2.String())).Exec()
	if err != nil {
		t.Error("Insert failed: " + err.Error())
		return
	}
	for i := 0; i < 1000; i++ {
		err = hs.Session.Query(fmt.Sprintf(TemplateInsertAccountActionTrace, testAccount, shard2.String(), globalSeq, blockTime.String())).Exec()
		if err != nil {
			t.Error("Insert failed: " + err.Error())
			return
		}
		var doc ActionTraceDoc
		doc.Receipt = make(map[string]interface{})
		doc.Receipt["global_sequence"] = globalSeq
		b, err := json.Marshal(doc)
		if err != nil {
			t.Error("Marshal failed: " + err.Error())
			return
		}
		err = hs.Session.Query(fmt.Sprintf(TemplateInsertActionTrace, globalSeq, string(b))).Exec()
		if err != nil {
			t.Error("Insert failed: " + err.Error())
			return
		}

		accountActionSeq := globalSeq - 1
		action := storage.Action{ GlobalActionSeq:globalSeq, AccountActionSeq:&accountActionSeq }
		fullTestAccountHistory = append(fullTestAccountHistory, action)

		globalSeq += 1
		blockTime.Time = blockTime.Add(time.Second)
	}

	notExistingAccount := "not existing account"
	r := TimestampRange{}
	expectedRecords := []storage.Action{}
	t.Run("should return empty set",
		getGetAccountHistoryTester(hs, notExistingAccount, 0, 0, r, nil, expectedRecords))
	t.Run("should return empty set (empty account)",
		getGetAccountHistoryTester(hs, notExistingAccount, 0, 10, r, nil, expectedRecords))
	t.Run("should return empty set (zero count)",
		getGetAccountHistoryTester(hs, testAccount, 1, 0, r, nil, expectedRecords))
	t.Run("should return all actions",
		getGetAccountHistoryTester(hs, testAccount, 0, 0, r, nil, fullTestAccountHistory))
}

func getGetAccountHistoryTester(hs *TestCassandraStorage, account string, pos int64, count int64, timeRange Range, dataFilter *DataFilter, expected []storage.Action) func (*testing.T) {
	return func(t *testing.T) {
		actionTraces, err := hs.getAccountHistory(account, pos, count, timeRange, dataFilter)
		if err != nil {
			t.Error("getAccountHistory failed: " + err.Error())
			return
		}
		if len(actionTraces) != len(expected) {
			t.Error("Wrong result array length.", "Got:", len(actionTraces), "Expected:", len(expected))
			return
		}
		for i, at := range actionTraces {
			gs1, ok := at.GlobalActionSeq.(float64)
			if !ok {
				t.Error("Unexpected GlobalActionSeq type")
			}
			gs2, ok := expected[i].GlobalActionSeq.(uint64)
			if !ok {
				t.Error("Unexpected GlobalActionSeq1 type")
			}
			if uint64(gs1) != uint64(gs2) {
				t.Error(fmt.Sprintf("Wrong global_seq for %d account action trace.", i), "Got:", uint64(gs1), "Expected:", uint64(gs2))
			}
			if *at.AccountActionSeq != *expected[i].AccountActionSeq {
				t.Error(fmt.Sprintf("Wrong AccountActionSeq for %d account action trace.", i), "Got:", *at.AccountActionSeq, "Expected:", *expected[i].AccountActionSeq)
			}
			if at.BlockNum != expected[i].BlockNum {
				t.Error(fmt.Sprintf("Wrong block_num for %d account action trace.", i), "Got:", at.BlockNum, "Expected:", expected[i].BlockNum)
			}
			if at.BlockTime != expected[i].BlockTime {
				t.Error(fmt.Sprintf("Wrong block_time for %d account action trace.", i), "Got:", at.BlockTime, "Expected:", expected[i].BlockTime)
			}
			//if at.ActionTrace != expected[i].ActionTrace {
			//	t.Error(fmt.Sprintf("Wrong ActionTrace for %d account action trace.", i), "Got:", at.ActionTrace, "Expected:", expected[i].ActionTrace)
			//}
		}
	}
}