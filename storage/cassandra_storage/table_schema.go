package cassandra_storage

import (
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
	Receipt          map[string]interface{} `json:"receipt"`
	Act              map[string]interface{} `json:"act"`
	ContextFree                 interface{} `json:"context_free"`
	Elapsed                     interface{} `json:"elapsed"`
	Console                     interface{} `json:"console"`
	TrxId                       interface{} `json:"trx_id"`
	BlockNum                    interface{} `json:"block_num"`
	BlockTime                   interface{} `json:"block_time"`
	ProducerBlockId             interface{} `json:"producer_block_id"`
	AccountRamDeltas            interface{} `json:"account_ram_deltas"`
	Except                      interface{} `json:"except"`
	InlineTraces           []ActionTraceDoc `json:"inline_traces"`
}

func (doc *ActionTraceDoc) GetTrace(target uint64) *ActionTraceDoc {
	gs := doc.Receipt["global_sequence"]
	if ui, ok := gs.(float64); ok {
		if uint64(ui) == target {
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
	} else {
		log.Println("unexpected type: global_seq")
	}

	for _, inline := range doc.InlineTraces {
		res := inline.GetTrace(target)
		if res != nil {
			return res
		}
	}
	return nil
}