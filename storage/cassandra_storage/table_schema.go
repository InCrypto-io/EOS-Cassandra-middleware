package cassandra_storage

import (
	"fmt"
	"time"
)


type Timestamp struct {
	time.Time
}

func (t *Timestamp) String() string {
	return t.Time.Format("2006-01-02T15:04:05") + fmt.Sprintf(".%03d+0000", t.Time.Nanosecond() / int(time.Millisecond))
}


type AccountActionTraceShardRecord struct {
	AccountName string
	ShardId     Timestamp
}


type AccountActionTraceRecord struct {
	AccountName string
	ShardId     Timestamp
	BlockTime   Timestamp
	GlobalSeq   uint64
	Parent      *uint64
}