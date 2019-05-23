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

	blockTime.Time = blockTime.Add(time.Hour)
	shard2 := blockTime
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
	expectedRecords := []AccountActionTraceRecord{}
	t.Run("should return empty set (empty account)",
		getGetAccountActionTracesTester(hs, "", defaultShards, defaultRange, true, 0, 0, expectedRecords))
	t.Run("should return empty set (empty shards)",
		getGetAccountActionTracesTester(hs, testAccount, []Timestamp{}, defaultRange, true, 0, 0, expectedRecords))
	r = NewTimestampRange(&shard1, true, &shard1, true)
	t.Run("should return empty set (empty range)",
		getGetAccountActionTracesTester(hs, testAccount, defaultShards, r, true, 0, 0, expectedRecords))
	record := AccountActionTraceRecord{ AccountName: testAccount, GlobalSeq: 11, ShardId: shard1, BlockTime: Timestamp{ Time: shard1.Add(time.Minute) }, Parent: nil }
	expectedRecords = []AccountActionTraceRecord{ record }
	t.Run("should return single trace",
		getGetAccountActionTracesTester(hs, testAccount, defaultShards, defaultRange, true, 0, 1, expectedRecords))
	r = NewTimestampRange(&shard1, false, &shard2, false)
	record = AccountActionTraceRecord{ AccountName: testAccount, GlobalSeq: 10000, ShardId: shard1, BlockTime: Timestamp{ Time: shard1.Add(999 * time.Minute) }, Parent: nil }
	expectedRecords = []AccountActionTraceRecord{ record }
	record = AccountActionTraceRecord{ AccountName: testAccount, GlobalSeq: 10001, ShardId: shard2, BlockTime: shard2, Parent: nil }
	expectedRecords = append(expectedRecords, record)
	t.Run("should return two traces from different shards (pos=999, count=2)",
		getGetAccountActionTracesTester(hs, testAccount, defaultShards, r, true, 9999, 2, expectedRecords))
	r = NewTimestampRange(&Timestamp{ Time: shard1.Add(999 * time.Minute) }, false, &shard2, false)
	t.Run("should return two traces from different shards (range that covering 10 last traces from one shard and 10 first traces from second shard + pos=9, count=2)",
		getGetAccountActionTracesTester(hs, testAccount, defaultShards, r, true, 9, 2, expectedRecords))
}

func getGetAccountActionTracesTester(hs *TestCassandraStorage, account string, shards []Timestamp, blockTimeRange Range, order bool, pos int64, count int64, expected []AccountActionTraceRecord) func (*testing.T) {
	return func(t *testing.T) {
		accountActionTraces, err := hs.getAccountActionTraces(account, shards, blockTimeRange, order, pos, count)
		if err != nil {
			t.Error("getAccountActionTraces failed: " + err.Error())
			return
		}
		if len(accountActionTraces) != len(expected) {
			t.Error("Wrong result array length.", "Got:", len(accountActionTraces), "Expected:", len(expected))
			return
		}
		for i, aat := range accountActionTraces {
			if aat.AccountName != expected[i].AccountName {
				t.Error(fmt.Sprintf("Wrong account name for %d account action trace.", i), "Got:", aat.AccountName, "Expected:", expected[i].AccountName)
			}
			if aat.GlobalSeq != expected[i].GlobalSeq {
				t.Error(fmt.Sprintf("Wrong global_seq for %d account action trace.", i), "Got:", aat.GlobalSeq, "Expected:", expected[i].GlobalSeq)
			}
			if aat.BlockTime != expected[i].BlockTime {
				fmt.Println(aat)
				t.Error(fmt.Sprintf("Wrong block time for %d account action trace.", i), "Got:", aat.BlockTime, "Expected:", expected[i].BlockTime)
			}
			if aat.Parent != expected[i].Parent {
				t.Error(fmt.Sprintf("Wrong parent for %d account action trace.", i), "Got:", aat.Parent, "Expected:", expected[i].Parent)
			}
			if aat.ShardId != expected[i].ShardId {
				t.Error(fmt.Sprintf("Wrong shard_id for %d account action trace.", i), "Got:", aat.ShardId, "Expected:", expected[i].ShardId)
			}
		}
	}
}