package cassandra_storage

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
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


type ActionTraceRecord struct {
	GlobalSeq uint64
	Doc       ActionTraceDoc
	Parent    *uint64
}


type ActionTraceDoc struct {
	Receipt          map[string]json.RawMessage `json:"receipt"`
	Act              map[string]json.RawMessage `json:"act"`
	ContextFree                     interface{} `json:"context_free"`
	Elapsed                         interface{} `json:"elapsed"`
	Console                         interface{} `json:"console"`
	TrxId                           interface{} `json:"trx_id"`
	BlockNum                        interface{} `json:"block_num"`
	BlockTime                       interface{} `json:"block_time"`
	ProducerBlockId                 interface{} `json:"producer_block_id"`
	AccountRamDeltas                interface{} `json:"account_ram_deltas"`
	Except                          interface{} `json:"except"`
	InlineTraces               []ActionTraceDoc `json:"inline_traces"`
}

func (doc *ActionTraceDoc) GetTrace(target uint64) *ActionTraceDoc {
	var gs interface{}
	err := json.Unmarshal(doc.Receipt["global_sequence"], &gs)
	if err != nil {
		log.Println("Unexpected Unmarshall error. " + err.Error())
		return nil
	}
	if ui, ok := gs.(uint64); ok {
		if ui == target {
			return doc
		}
	} else if s, ok := gs.(string); ok {
		ui, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil
		}
		if ui == target {
			return doc
		}
	}

	for _, inline := range doc.InlineTraces {
		res := inline.GetTrace(target)
		if res != nil {
			return res
		}
	}
	return nil
}