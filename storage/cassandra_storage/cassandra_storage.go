package cassandra_storage

import (
	"EOS-Cassandra-middleware/storage"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"sort"
	"strconv"
	"time"
)

const (
	//tables
	TableAccount                   = "account"
	TableAccountPublicKey          = "account_public_key"
	TableAccountControllingAccount = "account_controlling_account"
	TableAccountActionTrace        = "account_action_trace"
	TableAccountActionTraceShard   = "account_action_trace_shard"
	TableDateActionTrace           = "date_action_trace"
	TableActionTrace               = "action_trace"
	TableBlock                     = "block"
	TableLib                       = "lib"
	TableTransaction               = "transaction"
	TableTransactionTrace          = "transaction_trace"

	TracesPerShard = 10000

	TemplateErrorCassandraQueryFailed = "request to Cassandra failed: %s. Query: %s"
)


func countShards(pos int64, count int64, order bool) int64 {
	maxShards := int64(0)
	if !order {
		maxShards += 1
	}
	maxShards += (pos / int64(TracesPerShard))
	maxShards += (count / int64(TracesPerShard))
	remainder1 := pos % int64(TracesPerShard)
	remainder2 := count % int64(TracesPerShard)
	if remainder1 + remainder2 > TracesPerShard {
		maxShards += 2
	} else if remainder1 + remainder2 > 0 {
		maxShards += 1
	}
	return maxShards
}


type CassandraStorage struct {
	storage.IHistoryStorage

	Session *gocql.Session
}


func NewCassandraStorage(address string, keyspace string) (*CassandraStorage, error) {
	cluster := gocql.NewCluster(address)
	cluster.Keyspace       = keyspace
	cluster.ConnectTimeout = 5 * time.Second
	cluster.Timeout        = 10 * time.Second
	s, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	log.Println("cassandra init done")
	return &CassandraStorage{ Session: s }, nil
}


func (cs *CassandraStorage) Close() {
	cs.Session.Close()
}

func (cs *CassandraStorage) GetActions(args storage.GetActionArgs) (storage.GetActionsResult, error) {
	result := storage.GetActionsResult{ Actions: make([]storage.Action, 0) }

	lib, err := cs.getLastIrreversibleBlock()
	if err != nil {
		log.Println(fmt.Sprintf("Error from GetActions(): %s. Continuing execution.", err.Error()))
	}
	result.LastIrreversibleBlock = lib

	if args.AccountName == "" {
		return result, nil
	}

	pos, count, order := args.Normalize()
	log.Println("normalized: ", pos, count, order)
	if count == 0 {
		return result, nil
	}
	shardRecords, err := cs.getAccountShards(args.AccountName, TimestampRange{}, order, 0)//countShards(pos, count, order))
	if err != nil {
		return result, err
	}
	totalShards := len(shardRecords)
	if maxShards := countShards(pos, count, order); maxShards > int64(len(shardRecords)) {
		shardRecords = shardRecords[:maxShards]
	}
	log.Println("shards: ", shardRecords)
	shards := make([]Timestamp, len(shardRecords))
	for i, shard := range shardRecords {
		shards[i] = shard.ShardId
	}
	accountActionTraces, err := cs.getAccountActionTraces(args.AccountName, shards, TimestampRange{}, order, pos, count)
	if err != nil {
		return result, err
	}
	log.Println("accountActionTraces: ", accountActionTraces)
	if len(accountActionTraces) == 0 {
		return result, nil
	}
	//This is needed only for reverse history request
	lastTrace := &accountActionTraces[0]
	lastShardTracesCount := uint64(0)
	c, err := cs.countAccountActionTraces(args.AccountName, []Timestamp{ shards[0] }, NewTimestampExact(&lastTrace.BlockTime), lastTrace.GlobalSeq)
	if err != nil {
		return result, err
	}
	lastShardTracesCount += c
	c, err = cs.countAccountActionTraces(args.AccountName, []Timestamp{ shards[0] }, NewTimestampRange(nil, false, &lastTrace.BlockTime, true), 0)
	if err != nil {
		return result, err
	}
	lastShardTracesCount += c
	lastAccountSeq := (lastShardTracesCount - 1) + (uint64(totalShards - 1) * uint64(TracesPerShard))
	//============================================

	globalSequences := make([]uint64, 0)
	lastGlobalSeq := uint64(0)
	for _, aat := range accountActionTraces {
		gs := aat.GlobalSeq
		if aat.Parent != nil {
			gs = *aat.Parent
		}
		if gs != lastGlobalSeq {
			globalSequences = append(globalSequences, gs)
			lastGlobalSeq = gs
		}
	}
	actionTraces, err := cs.getActionTraces(globalSequences, order)
	if err != nil {
		return result, err
	}
	log.Println(fmt.Sprintf("Found %d traces", len(actionTraces)))
	if len(actionTraces) == 0 {
		return result, nil
	}
	for i, aat := range accountActionTraces {
		var doc *ActionTraceDoc
		if aat.Parent == nil {
			id := 0
			for i, at := range actionTraces {
				if aat.GlobalSeq == at.GlobalSeq {
					id = i
					doc = &at.Doc
					break
				}
			}
			actionTraces = actionTraces[id:]
		} else {
			id := 0
			for i, at := range actionTraces {
				if inline := at.Doc.GetTrace(aat.GlobalSeq); inline != nil {
					id = i
					doc = inline
					break
				}
			}
			actionTraces = actionTraces[id:]
		}
		if doc == nil {
			log.Println(fmt.Sprintf("Action trace %d not found", aat.GlobalSeq))
			continue
		}

		bytes, err := json.Marshal(doc)
		if err != nil {
			log.Println(fmt.Sprintf("Failed to encode trace %d. Error: %s", aat.GlobalSeq, err.Error()))
			continue
		}
		accountActionSeq := uint64(0)
		if order {
			accountActionSeq = uint64(pos) + uint64(i)
		} else {
			accountActionSeq = lastAccountSeq - uint64(i)
		}
		action := storage.Action{ GlobalActionSeq: doc.Receipt["global_sequence"], AccountActionSeq: accountActionSeq,
			BlockNum: doc.BlockNum, BlockTime: doc.BlockTime,
			ActionTrace: bytes }
		result.Actions = append(result.Actions, action)
	}
	if len(result.Actions) != len(accountActionTraces) {
		log.Println("Warning! Missing traces")
	}
	return result, nil
}

func (cs *CassandraStorage) GetTransaction(args storage.GetTransactionArgs) (storage.GetTransactionResult, error) {
	result := storage.GetTransactionResult{}
	lib, err := cs.getLastIrreversibleBlock()
	if err != nil {
		log.Println(fmt.Sprintf("Error from GetTransaction(): %s. Continuing execution.", err.Error()))
	}
	result.LastIrreversibleBlock = lib

	if args.ID == "" {
		return result, errors.New("Invalid transaction ID: " + args.ID)
	}

	//TODO: make request to cassandra
	return result, nil
}

func (cs *CassandraStorage) GetKeyAccounts(args storage.GetKeyAccountsArgs) (storage.GetKeyAccountsResult, error) {
	result := storage.GetKeyAccountsResult{ AccountNames: make([]string, 0) }

	if args.PublicKey == "" {
		return result, nil
	}

	//TODO: make request to cassandra
	return result, nil
}

func (cs *CassandraStorage) GetControlledAccounts(args storage.GetControlledAccountsArgs) (storage.GetControlledAccountsResult, error) {
	result := storage.GetControlledAccountsResult{ ControlledAccounts: make([]string, 0) }

	if args.ControllingAccount == "" {
		return result, nil
	}

	//TODO: make request to cassandra
	return result, nil
}


//private
func (cs *CassandraStorage) getAccountActionTraces(account string, shards []Timestamp, blockTimeRange Range, order bool, pos int64, limit int64) ([]AccountActionTraceRecord, error) {
	records := make([]AccountActionTraceRecord, 0)
	traceSkip := int64(0)
	if order {
		shardSkip := pos / int64(TracesPerShard)
		if shardSkip > int64(len(shards)) {
			return records, nil
		}
		shards = shards[shardSkip:]
		traceSkip = pos % int64(TracesPerShard)
	}
	withLimit := limit > 0
	orderStr := "ASC"
	if !order {
		orderStr = "DESC"
	}
	rangeStr := ""
	if !blockTimeRange.IsEmpty() {
		rangeStr += "AND " + blockTimeRange.Format("block_time")
	}
	for _, shard := range shards {
		query := fmt.Sprintf("SELECT * FROM %s WHERE account_name='%s' AND shard_id='%s' %s ORDER BY block_time %s, global_seq %s ",
			TableAccountActionTrace, account, shard.String(), rangeStr, orderStr, orderStr)
		if withLimit {
			query += fmt.Sprintf(" LIMIT %d", limit)
		}
		fmt.Println("Query: ", query)
		var r AccountActionTraceRecord
		iter := cs.Session.Query(query).Iter()
		for !withLimit || (limit > 0 && iter.Scan(&r.AccountName, &r.ShardId.Time, &r.BlockTime.Time, &r.GlobalSeq, &r.Parent)) {
			if traceSkip > 0 {
				traceSkip -= 1
			} else {
				records = append(records, r)
				limit -= 1
			}
		}
		if err := iter.Close(); err != nil {
			err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
			log.Println("Error from getAccountActionTraces: " + err.Error())
			return records, err
		}
		if withLimit && limit == 0 {
			break
		}
	}
	return records, nil
}

func (cs *CassandraStorage) getAccountShards(account string, shardRange Range, order bool, limit int64) ([]AccountActionTraceShardRecord, error) {
	records := make([]AccountActionTraceShardRecord, 0)
	orderStr := "ASC"
	if !order {
		orderStr = "DESC"
	}
	rangeStr := ""
	if !shardRange.IsEmpty() {
		rangeStr += "AND " + shardRange.Format("shard_id")
	}
	query := fmt.Sprintf("SELECT shard_id FROM %s WHERE account_name='%s' %s ORDER BY shard_id %s", TableAccountActionTraceShard, account, rangeStr, orderStr)
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	fmt.Println("Query: ", query)
	var r AccountActionTraceShardRecord
	iter := cs.Session.Query(query).Iter()
	for iter.Scan(&r.ShardId.Time) {
		records = append(records, r)
	}
	if err := iter.Close(); err != nil {
		err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
		log.Println("Error from getAccountShards: " + err.Error())
		return records, err
	}
	return records, nil
}

func (cs *CassandraStorage) getActionTraces(globalSequences []uint64, order bool) ([]ActionTraceRecord, error) {
	records := make([]ActionTraceRecord, 0)
	if len(globalSequences) == 0 {
		return records, nil
	}
	inClause := " global_seq IN ("
	for _, gs := range globalSequences[:len(globalSequences)-1] {
		inClause += strconv.FormatUint(gs, 10) + ", "
	}
	inClause += strconv.FormatUint(globalSequences[len(globalSequences)-1], 10) + ")"
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s", TableActionTrace, inClause)
	fmt.Println("Query: ", query)
	var r ActionTraceRecord
	var doc string
	iter := cs.Session.Query(query).Iter()
	for iter.Scan(&r.GlobalSeq, &doc, &r.Parent) {
		err := json.Unmarshal([]byte(doc), &r.Doc)
		if err != nil {
			log.Println(fmt.Sprintf("Error from getActionTraces. Failed to unmarshal action_trace %s: %s", doc, err.Error()))
			continue
		}
		records = append(records, r)
		r = ActionTraceRecord{}
	}
	if err := iter.Close(); err != nil {
		err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
		log.Println("Error from getActionTraces: " + err.Error())
		return records, err
	}
	if len(globalSequences) != len(records) {
		log.Println("Warning! Not all traces found. Query: " + query) //TODO: log missing global_seq
	}

	sort.Slice(records, func(i, j int) bool {
		if order {
			return records[i].GlobalSeq < records[j].GlobalSeq
		} else {
			return records[j].GlobalSeq < records[i].GlobalSeq
		}
	})
	return records, nil
}

func (cs *CassandraStorage) getLastIrreversibleBlock() (uint64, error) {
	query := fmt.Sprintf("SELECT block_num FROM %s WHERE part_key=0", TableLib)

	var lib uint64
	if err := cs.Session.Query(query).Scan(&lib); err != nil {
		err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
		log.Println("Error from getActionTraces: " + err.Error())
		return 0, err
	}
	return lib, nil
}

func (cs *CassandraStorage) countAccountActionTraces(account string, shards []Timestamp, blockTimeRange Range, top uint64) (uint64, error) {
	count := uint64(0)
	rangeStr := ""
	if !blockTimeRange.IsEmpty() {
		rangeStr += "AND " + blockTimeRange.Format("block_time")
	}
	var tmpCount uint64
	for _, shard := range shards {
		query := fmt.Sprintf("SELECT count(*) FROM %s WHERE account_name='%s' AND shard_id='%s' %s ",
			TableAccountActionTrace, account, shard.String(), rangeStr)
		if top != 0 {
			query += fmt.Sprintf(" AND global_seq <= %d ", top)
		}
		fmt.Println("Query: ", query)
		if err := cs.Session.Query(query).Scan(&tmpCount); err != nil {
			err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
			log.Println("Error from countAccountActionTraces: " + err.Error())
			return 0, err
		}
		count += tmpCount
	}
	return count, nil
}