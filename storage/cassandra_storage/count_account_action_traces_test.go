package cassandra_storage

import (
	"fmt"
	"testing"
	"time"
)


func Test_countAccountActionTraces(t *testing.T) {
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
	emptyShard := Timestamp{ Time: shardTime }
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

	var r Range
	r = TimestampRange{}
	t.Run("multishard count",
		getCountAccountActionTracesTester(hs, testAccount, []Timestamp{ shard1, shard2 }, r, 0, 11000))
	t.Run("full shard count",
		getCountAccountActionTracesTester(hs, testAccount, []Timestamp{ shard1 }, r, 0, TracesPerShard))
	t.Run("non-full shard count",
		getCountAccountActionTracesTester(hs, testAccount, []Timestamp{ shard2 }, r, 0, 1000))
	t.Run("empty shard count",
		getCountAccountActionTracesTester(hs, testAccount, []Timestamp{ emptyShard }, r, 0, 0))
	r = NewTimestampRange(&shard1, true, &Timestamp{ Time: shard1.Add(time.Minute) }, true)
	t.Run("non-traces range",
		getCountAccountActionTracesTester(hs, testAccount, []Timestamp{ emptyShard }, r, 0, 0))
	r = NewTimestampRange(&shard1, true, &Timestamp{ Time: shard1.Add(time.Minute) }, false)
	t.Run("should return 10",
		getCountAccountActionTracesTester(hs, testAccount, []Timestamp{ shard1 }, r, 0, 10))
	r = NewTimestampExact(&Timestamp{ Time: shard1.Add(time.Hour) })
	t.Run("should return 10",
		getCountAccountActionTracesTester(hs, testAccount, []Timestamp{ shard1 }, r, 666, 10))
	r = NewTimestampExact(&Timestamp{ Time: shard1.Add(time.Minute) })
	t.Run("should return 1",
		getCountAccountActionTracesTester(hs, testAccount, []Timestamp{ shard1 }, r, 11, 1))
}

func getCountAccountActionTracesTester(hs *TestCassandraStorage, account string, shards []Timestamp, blockTimeRange Range, top uint64, expected uint64) func (*testing.T) {
	return func(t *testing.T) {
		c, err := hs.countAccountActionTraces(account, shards, blockTimeRange, top)
		if err != nil {
			t.Error("countAccountActionTraces failed: " + err.Error())
			return
		}
		if c != expected {
			t.Error("Got: ", c, "Expected: ", expected)
		}
	}
}