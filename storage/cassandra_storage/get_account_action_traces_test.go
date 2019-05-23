package cassandra_storage

import (
	"fmt"
	"testing"
	"time"
)


func Test_getAccountActionTraces(t *testing.T) {
	hs, err := NewTestCassandraStorage()
	if err != nil {
		t.Error("Failed to create history storage object: " + err.Error())
		return
	}
	defer hs.Close()

	err = hs.Session.Query(fmt.Sprintf("CREATE TABLE %s (shard_id timestamp, account_name text, global_seq varint, block_time timestamp, parent varint," +
		" PRIMARY KEY((account_name, shard_id), block_time, global_seq))", TableAccountActionTrace)).Exec()
	if err != nil {
		t.Error("Failed to create table: " + err.Error())
		return
	}

	testAccount := "test"
	globalSeq := uint64(1)
	shardTime, err := time.Parse("2006-01-02T15:04:05", "2018-06-09T11:11:11")
	if err != nil {
		t.Error("Failed to create shard: " + err.Error())
		return
	}
	//emptyShard := Timestamp{ Time: shardTime }
	shardTime = shardTime.Add(time.Millisecond * 500)
	shard1 := Timestamp{ Time: shardTime }
	blockTime := shard1
	for i := 0; i < 1000; i++ { //for 1000 minutes
		for j := 0; j < 10; j++ { //insert 10 records per minute
			err = hs.Session.Query(fmt.Sprintf(TemplateInsertAccountActionTrace, testAccount, shard1.String(), globalSeq, blockTime.String())).Exec()
			if err != nil {
				t.Error("Insert failed: " + err.Error())
				return
			}
			globalSeq += 1
		}
		blockTime.Time = blockTime.Add(time.Minute)
	}

	shard2 := blockTime
	shard2.Time = shard2.Add(time.Hour)
	for i := 0; i < 1000; i++ {
		err = hs.Session.Query(fmt.Sprintf(TemplateInsertAccountActionTrace, testAccount, shard2.String(), globalSeq, blockTime.String())).Exec()
		if err != nil {
			t.Error("Insert failed: " + err.Error())
			return
		}
		globalSeq += 1
		blockTime.Time = blockTime.Add(time.Second)
	}

	defaultShards := []Timestamp{ shard1, shard2 }
	defaultRange := NewTimestampRange(&shard1, true, &shard2, true)
	var r Range
	t.Run("should return empty set (empty account)",
		getGetAccountActionTracesTester(hs, "", defaultShards, defaultRange, true, 0, 0, []AccountActionTraceRecord{}))
	t.Run("should return empty set (empty shards)",
		getGetAccountActionTracesTester(hs, testAccount, []Timestamp{}, defaultRange, true, 0, 0, []AccountActionTraceRecord{}))
	r = NewTimestampRange(&shard1, true, &shard1, true)
	t.Run("should return empty set (empty range)",
		getGetAccountActionTracesTester(hs, testAccount, defaultShards, r, true, 0, 0, []AccountActionTraceRecord{}))
	record := AccountActionTraceRecord{ AccountName: testAccount, GlobalSeq: 1, ShardId: shard1, BlockTime: shard1, Parent: nil }
	t.Run("should return single trace",
		getGetAccountActionTracesTester(hs, testAccount, defaultShards, defaultRange, true, 0, 1, []AccountActionTraceRecord{ record }))
}

func getGetAccountActionTracesTester(hs *TestCassandraStorage, account string, shards []Timestamp, blockTimeRange Range, order bool, pos int64, limit int64, expected []AccountActionTraceRecord) func (*testing.T) {
	return func(t *testing.T) {
		accountActionTraces, err := hs.getAccountActionTraces(account, shards, blockTimeRange, order, pos, limit)
		if err != nil {
			t.Error("getAccountActionTraces failed: " + err.Error())
			return
		}
		if len(accountActionTraces) != len(expected) {
			t.Error("Wrong result array length")
			return
		}
		for i, aat := range accountActionTraces {
			if aat.AccountName != expected[i].AccountName {
				t.Error("Wrong account name.", "Got:", aat.AccountName, "Expected:", expected[i].AccountName)
			}
			if aat.GlobalSeq != expected[i].GlobalSeq {
				t.Error("Wrong global_seq.", "Got:", aat.GlobalSeq, "Expected:", expected[i].GlobalSeq)
			}
			if aat.BlockTime != expected[i].BlockTime {
				t.Error("Wrong block time.", "Got:", aat.BlockTime, "Expected:", expected[i].BlockTime)
			}
			if aat.Parent != expected[i].Parent {
				t.Error("Wrong parent.", "Got:", aat.Parent, "Expected:", expected[i].Parent)
			}
			if aat.ShardId != expected[i].ShardId {
				t.Error("Wrong shard_id.", "Got:", aat.ShardId, "Expected:", expected[i].ShardId)
			}
		}
	}
}