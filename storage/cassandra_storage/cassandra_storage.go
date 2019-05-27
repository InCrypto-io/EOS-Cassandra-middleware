package cassandra_storage

import (
	"EOS-Cassandra-middleware/storage"
	"EOS-Cassandra-middleware/utility"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"sort"
	"strconv"
	"strings"
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
	if order {
		result.Actions, err = cs.getAccountHistory(args.AccountName, pos, count)
	} else {
		result.Actions, err = cs.getAccountHistoryReverse(args.AccountName, pos, count)
	}
	return result, err
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

	transaction, err := cs.getTransaction(args.ID)
	if err != nil {
		return result, err
	}
	transactionTrace, err := cs.getTransactionTrace(args.ID)
	if err != nil {
		return result, err
	}
	expandedTraces := make([]*ActionTraceDoc, 0)
	for _, t := range transactionTrace.Doc.ActionTraces {
		expandedTraces = append(expandedTraces, t.ExpandTraces()...)
	}
	for _, t := range expandedTraces {
		result.Traces = append(result.Traces, *t)
	}
	result.ID = transaction.ID
	result.BlockNum = transactionTrace.Doc.BlockNum
	result.BlockTime = transactionTrace.Doc.BlockTime
	result.Trx = make(map[string]interface{})
	result.Trx["trx"] = transaction.Doc
	result.Trx["receipt"] = transactionTrace.Doc.Receipt
	if blockId, ok := transactionTrace.Doc.ProducerBlockId.(string); ok {
		block, err := cs.getBlock(blockId)
		if err != nil {
			return result, nil
		}
		if transactionsObj, ok := block.Doc.Block["transactions"]; ok {
			if transactions, ok := transactionsObj.([]interface{}); ok {
				for _, t := range transactions {
					if m, ok := t.(map[string]interface{});ok {
						if v, ok := m["trx"].([]interface{}); ok {
							if len(v) < 2 {
								return result, nil
							}
							receipt := result.Trx["receipt"]
							trx := v[1]
							if s, ok := trx.(string); ok {
								if s != args.ID {
									continue
								}
								if m, ok := receipt.(map[string]interface{}); ok {
									m["trx"] = v
									result.Trx["receipt"] = m
								}
							} else if m, ok := trx.(map[string]interface{}); ok {
								compressionObj, _ := m["compression"]
								if compression, ok := compressionObj.(string); ok {
									if compression != "none" {
										continue
									}
									if packed, ok := m["packed_trx"].(string); ok {
										toFind := ""
										if hex, ok := transactionTrace.Doc.ActionTraces[0].Act["hex_data"]; ok {
											toFind, _ = hex.(string)
										} else if data, ok := transactionTrace.Doc.ActionTraces[0].Act["data"]; ok {
											toFind, _ = data.(string)
										}
										if strings.Contains(packed, toFind) {
											if m, ok := receipt.(map[string]interface{}); ok {
												m["trx"] = v
												result.Trx["receipt"] = m
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return result, nil
}

func (cs *CassandraStorage) GetKeyAccounts(args storage.GetKeyAccountsArgs) (storage.GetKeyAccountsResult, error) {
	result := storage.GetKeyAccountsResult{ AccountNames: make([]string, 0) }

	if args.PublicKey == "" {
		return result, nil
	}

	accounts, err := cs.getKeyAccounts(args.PublicKey)
	if err != nil {
		return result, err
	}
	accounts = utility.Unique(accounts)
	sort.Strings(accounts)
	result.AccountNames = accounts
	return result, nil
}

func (cs *CassandraStorage) GetControlledAccounts(args storage.GetControlledAccountsArgs) (storage.GetControlledAccountsResult, error) {
	result := storage.GetControlledAccountsResult{ ControlledAccounts: make([]string, 0) }

	if args.ControllingAccount == "" {
		return result, nil
	}

	accounts, err := cs.getControlledAccounts(args.ControllingAccount)
	if err != nil {
		return result, err
	}
	accounts = utility.Unique(accounts)
	sort.Strings(accounts)
	result.ControlledAccounts = accounts
	return result, nil
}


//getAccountHistory is a handler for get_actions request with pos != -1
func (cs *CassandraStorage) getAccountHistory(account string, pos int64, count int64) ([]storage.Action, error) {
	result := make([]storage.Action, 0)
	order := true
	shardRecords, err := cs.getAccountShards(account, TimestampRange{}, order, countShards(pos, count, order))
	if err != nil {
		return result, err
	}
	log.Println("shards: ", shardRecords)
	shards := make([]Timestamp, len(shardRecords))
	for i, shard := range shardRecords {
		shards[i] = shard.ShardId
	}
	accountActionTraces, err := cs.getAccountActionTraces(account, shards, TimestampRange{}, order, pos, count)
	if err != nil {
		return result, err
	}
	log.Println("accountActionTraces: ", accountActionTraces)
	if len(accountActionTraces) == 0 {
		return result, nil
	}
	actionTraces, err := cs.getContainingActionTraces(accountActionTraces)
	if err != nil {
		return result, err
	}
	sort.Slice(actionTraces, func(i, j int) bool { return actionTraces[i].GlobalSeq < actionTraces[j].GlobalSeq })
	log.Println(fmt.Sprintf("Found %d traces", len(actionTraces)))
	if len(actionTraces) == 0 {
		return result, nil
	}
	for i, aat := range accountActionTraces {
		var doc *ActionTraceDoc
		id := 0
		if aat.Parent == nil {
			for i, at := range actionTraces {
				if aat.GlobalSeq == at.GlobalSeq {
					id = i
					doc = &at.Doc
					break
				}
			}
		} else {
			for i, at := range actionTraces {
				if inline := at.Doc.GetTrace(aat.GlobalSeq); inline != nil {
					id = i
					doc = inline
					break
				}
			}
		}
		actionTraces = actionTraces[id:]
		if doc == nil {
			log.Println(fmt.Sprintf("Warning! Action trace %d not found", aat.GlobalSeq))
			continue
		}

		bytes, err := json.Marshal(doc)
		if err != nil {
			log.Println(fmt.Sprintf("Failed to encode trace %d. Error: %s", aat.GlobalSeq, err.Error()))
			continue
		}
		accountActionSeq := uint64(pos) + uint64(i)
		action := storage.Action{ GlobalActionSeq: doc.Receipt["global_sequence"], AccountActionSeq: accountActionSeq,
			BlockNum: doc.BlockNum, BlockTime: doc.BlockTime,
			ActionTrace: bytes }
		result = append(result, action)
	}
	if len(result) != len(accountActionTraces) {
		log.Println("Warning! Missing traces")
	}
	return result, nil
}

//getAccountHistory is a handler for get_actions request with pos == -1
func (cs *CassandraStorage) getAccountHistoryReverse(account string, pos int64, count int64) ([]storage.Action, error) {
	result := make([]storage.Action, 0)
	order := false
	shardRecords, err := cs.getAccountShards(account, TimestampRange{}, order, 0)
	if err != nil {
		return result, err
	}
	totalShards := len(shardRecords)
	if maxShards := countShards(pos, count, order); maxShards < int64(len(shardRecords)) {
		shardRecords = shardRecords[:maxShards]
	}
	log.Println("shards: ", shardRecords)
	shards := make([]Timestamp, len(shardRecords))
	for i, shard := range shardRecords {
		shards[i] = shard.ShardId
	}
	accountActionTraces, err := cs.getAccountActionTraces(account, shards, TimestampRange{}, order, pos, count)
	if err != nil {
		return result, err
	}
	log.Println("accountActionTraces: ", accountActionTraces)
	if len(accountActionTraces) == 0 {
		return result, nil
	}

	lastTrace := &accountActionTraces[0]
	lastShardTracesCount := uint64(0)
	c, err := cs.countAccountActionTraces(account, []Timestamp{ shards[0] }, NewTimestampExact(&lastTrace.BlockTime), lastTrace.GlobalSeq)
	if err != nil {
		return result, err
	}
	lastShardTracesCount += c
	c, err = cs.countAccountActionTraces(account, []Timestamp{ shards[0] }, NewTimestampRange(nil, false, &lastTrace.BlockTime, true), 0)
	if err != nil {
		return result, err
	}
	lastShardTracesCount += c
	lastAccountSeq := (lastShardTracesCount - 1) + (uint64(totalShards - 1) * uint64(TracesPerShard))

	actionTraces, err := cs.getContainingActionTraces(accountActionTraces)
	if err != nil {
		return result, err
	}
	sort.Slice(actionTraces, func(i, j int) bool { return actionTraces[j].GlobalSeq < actionTraces[i].GlobalSeq })
	log.Println(fmt.Sprintf("Found %d traces", len(actionTraces)))
	if len(actionTraces) == 0 {
		return result, nil
	}
	for i, aat := range accountActionTraces {
		var doc *ActionTraceDoc
		id := 0
		if aat.Parent == nil {
			for i, at := range actionTraces {
				if aat.GlobalSeq == at.GlobalSeq {
					id = i
					doc = &at.Doc
					break
				}
			}
		} else {
			for i, at := range actionTraces {
				if inline := at.Doc.GetTrace(aat.GlobalSeq); inline != nil {
					id = i
					doc = inline
					break
				}
			}
		}
		actionTraces = actionTraces[id:]
		if doc == nil {
			log.Println(fmt.Sprintf("Action trace %d not found", aat.GlobalSeq))
			continue
		}

		bytes, err := json.Marshal(doc)
		if err != nil {
			log.Println(fmt.Sprintf("Failed to encode trace %d. Error: %s", aat.GlobalSeq, err.Error()))
			continue
		}
		accountActionSeq := lastAccountSeq - uint64(i)
		action := storage.Action{ GlobalActionSeq: doc.Receipt["global_sequence"], AccountActionSeq: accountActionSeq,
			BlockNum: doc.BlockNum, BlockTime: doc.BlockTime,
			ActionTrace: bytes }
		result = append(result, action)
	}
	if len(result) != len(accountActionTraces) {
		log.Println("Warning! Missing traces")
	}
	return result, nil
}


//private
//if order == false only works with pos == 0
func (cs *CassandraStorage) getAccountActionTraces(account string, shards []Timestamp, blockTimeRange Range, order bool, pos int64, count int64) ([]AccountActionTraceRecord, error) {
	records := make([]AccountActionTraceRecord, 0)
	traceSkip := pos
	if order {
		shardSkip := pos / int64(TracesPerShard)
		if shardSkip > int64(len(shards)) {
			return records, nil
		}
		shards = shards[shardSkip:]
		traceSkip = pos % int64(TracesPerShard)
	}
	limit := pos + count
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
		for (!withLimit || count > 0) && iter.Scan(&r.AccountName, &r.ShardId.Time, &r.BlockTime.Time, &r.GlobalSeq, &r.Parent) {
			if traceSkip > 0 {
				traceSkip -= 1
			} else {
				records = append(records, r)
				count -= 1
			}
		}
		if err := iter.Close(); err != nil {
			err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
			log.Println("Error from getAccountActionTraces: " + err.Error())
			return records, err
		}
		if withLimit && count == 0 {
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

func (cs *CassandraStorage) getActionTraces(globalSequences []uint64) ([]ActionTraceRecord, error) {
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
	return records, nil
}

func (cs *CassandraStorage) getBlock(id string) (*BlockRecord, error)  {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id='%s'", TableBlock, id)

	var record BlockRecord
	var doc string
	if err := cs.Session.Query(query).Scan(&record.ID, &record.BlockNum, &doc, &record.Irreversible); err != nil {
		err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
		log.Println("Error from getBlock: " + err.Error())
		return nil, err
	}

	err := json.Unmarshal([]byte(doc), &record.Doc)
	if err != nil {
		err = fmt.Errorf("failed to unmarshal block %s: %s", id, err.Error())
		log.Println(fmt.Sprintf("Error from getBlock: %s", err.Error()))
		return nil, err
	}
	return &record, nil
}

func (cs *CassandraStorage) getContainingActionTraces(accountActionTraces []AccountActionTraceRecord) ([]ActionTraceRecord, error) {
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
	actionTraces, err := cs.getActionTraces(globalSequences)
	return actionTraces, err
}

func (cs *CassandraStorage) getControlledAccounts(controllingAccount string) ([]string, error) {
	query := fmt.Sprintf("SELECT name FROM %s WHERE controlling_name='%s'", TableAccountControllingAccount, controllingAccount)

	accounts := make([]string, 0)
	var account string
	iter := cs.Session.Query(query).Iter()
	for iter.Scan(&account) {
		accounts = append(accounts, account)
	}
	if err := iter.Close(); err != nil {
		err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
		log.Println("Error from getControlledAccounts: " + err.Error())
		return accounts, err
	}
	return accounts, nil
}

func (cs *CassandraStorage) getKeyAccounts(key string) ([]string, error) {
	query := fmt.Sprintf("SELECT name FROM %s WHERE key='%s'", TableAccountPublicKey, key)

	accounts := make([]string, 0)
	var account string
	iter := cs.Session.Query(query).Iter()
	for iter.Scan(&account) {
		accounts = append(accounts, account)
	}
	if err := iter.Close(); err != nil {
		err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
		log.Println("Error from getKeyAccounts: " + err.Error())
		return accounts, err
	}
	return accounts, nil
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

func (cs *CassandraStorage) getTransaction(id string) (*TransactionRecord, error)  {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id='%s'", TableTransaction, id)

	var record TransactionRecord
	var doc string
	if err := cs.Session.Query(query).Scan(&record.ID, &doc); err != nil {
		err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
		log.Println("Error from getTransaction: " + err.Error())
		return nil, err
	}

	err := json.Unmarshal([]byte(doc), &record.Doc)
	if err != nil {
		err = fmt.Errorf("failed to unmarshal transaction %s: %s", id, err.Error())
		log.Println(fmt.Sprintf("Error from getTransaction: %s", err.Error()))
		return nil, err
	}
	return &record, nil
}

func (cs *CassandraStorage) getTransactionTrace(id string) (*TransactionTraceRecord, error)  {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id='%s'", TableTransactionTrace, id)

	var record TransactionTraceRecord
	var doc string
	if err := cs.Session.Query(query).Scan(&record.ID, &record.BlockDate, &record.BlockNum, &doc); err != nil {
		err = fmt.Errorf(TemplateErrorCassandraQueryFailed, err.Error(), query)
		log.Println("Error from getTransactionTrace: " + err.Error())
		return nil, err
	}

	err := json.Unmarshal([]byte(doc), &record.Doc)
	if err != nil {
		err = fmt.Errorf("failed to unmarshal transaction trace %s: %s", id, err.Error())
		log.Println(fmt.Sprintf("Error from getTransactionTrace: %s", err.Error()))
		return nil, err
	}
	return &record, nil
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