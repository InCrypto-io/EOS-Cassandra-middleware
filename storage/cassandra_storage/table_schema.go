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


type BlockRecord struct {
	ID           string
	BlockNum     uint64
	Doc          BlockDoc
	Irreversible *bool
}


type DateActionTraceRecord struct {
	BlockDate string
	BlockTime Timestamp
	GlobalSeq uint64
	Parent    *uint64
}


type TransactionRecord struct {
	ID  string
	Doc TransactionDoc
}


type TransactionTraceRecord struct {
	ID        string
	BlockDate string
	BlockNum  uint64
	Doc TransactionTraceDoc
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

func (doc *ActionTraceDoc) ExpandTraces() []*ActionTraceDoc {
	var traces []*ActionTraceDoc
	if doc == nil {
		return traces
	}

	tmp := []*ActionTraceDoc {doc}
	for len(tmp) > 0 {
		tPtr := tmp[0]
		tmp = tmp[1:]
		traces = append(traces, tPtr)

		var inlineTraces []*ActionTraceDoc
		for i, _ := range tPtr.InlineTraces {
			inlineTraces = append(inlineTraces, &tPtr.InlineTraces[i])
		}
		tmp = append(inlineTraces, tmp...)
	}
	return traces
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


type BlockDoc struct {
	ID                                          interface{} `json:"id"`
	BlockNum                                    interface{} `json:"block_num"`
	Header                                      interface{} `json:"header"`
	DposProposedIrreversibleBlocknum            interface{} `json:"dpos_proposed_irreversible_blocknum"`
	DposIrreversibleBlocknum                    interface{} `json:"dpos_irreversible_blocknum"`
	BftIrreversibleBlocknum                     interface{} `json:"bft_irreversible_blocknum"`
	PendingScheduleLibNum                       interface{} `json:"pending_schedule_lib_num"`
	PendingScheduleHash                         interface{} `json:"pending_schedule_hash"`
	PendingSchedule                             interface{} `json:"pending_schedule"`
	ActiveSchedule                              interface{} `json:"active_schedule"`
	BlockrootMerkle                             interface{} `json:"blockroot_merkle"`
	ProducerToLastProduced                      interface{} `json:"producer_to_last_produced"`
	ProducerToLastImpliedIrb                    interface{} `json:"producer_to_last_implied_irb"`
	BlockSigningKey                             interface{} `json:"block_signing_key"`
	ConfirmCount                                interface{} `json:"confirm_count"`
	Confirmations                               interface{} `json:"confirmations"`
	Block                            map[string]interface{} `json:"block"`
	Validated                                   interface{} `json:"validated"`
	InCurrentChain                              interface{} `json:"in_current_chain"`
}


type TransactionDoc struct {
	Expiration            interface{} `json:"expiration"`
	RefBlockNum           interface{} `json:"ref_block_num"`
	RefBlockPrefix        interface{} `json:"ref_block_prefix"`
	MaxNetUsageWords      interface{} `json:"max_net_usage_words"`
	MaxCpuUsageMs         interface{} `json:"max_cpu_usage_ms"`
	DelaySec              interface{} `json:"delay_sec"`
	ContextFreeActions    interface{} `json:"context_free_actions"`
	Actions               interface{} `json:"actions"`
	TransactionExtensions interface{} `json:"transaction_extensions"`
	Signatures            interface{} `json:"signatures"`
	ContextFreeData       interface{} `json:"context_free_data"`
}


type TransactionTraceDoc struct {
	ID                   interface{} `json:"id"`
	BlockNum             interface{} `json:"block_num"`
	BlockTime            interface{} `json:"block_time"`
	ProducerBlockId      interface{} `json:"producer_block_id"`
	Receipt              interface{} `json:"receipt"`
	Elapsed              interface{} `json:"elapsed"`
	NetUsage             interface{} `json:"net_usage"`
	Scheduled            interface{} `json:"scheduled"`
	ActionTraces    []ActionTraceDoc `json:"action_traces"`
	FailedDtrxTrace      interface{} `json:"failed_dtrx_trace"`
}