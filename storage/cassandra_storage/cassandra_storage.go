package cassandra_storage

import (
	"EOS-Cassandra-middleware/storage"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"log"
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
	cluster.Keyspace = keyspace
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
	//create empty range
	shardRecords := cs.getAccountShards(args.AccountName, TimestampRange{}, order, countShards(pos, count, order))
	log.Println("shards: ", shardRecords)
	shards := make([]Timestamp, len(shardRecords))
	for i, shard := range shardRecords {
		shards[i] = shard.ShardId
	}
	actionTraces := cs.getAccountActionTraces(args.AccountName, shards, TimestampRange{}, order, count)
	log.Println("actionTraces: ", actionTraces)
	//TODO: make request to cassandra
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
func (cs *CassandraStorage) getAccountActionTraces(account string, shards []Timestamp, blockTimeRange TimestampRange, order bool, limit int64) []AccountActionTraceRecord {
	records := make([]AccountActionTraceRecord, 0)
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
		query := fmt.Sprintf("SELECT * FROM %s WHERE account_name='%s' and shard_id='%s' %s ORDER BY block_time %s, global_seq %s ",
			TableAccountActionTrace, account, shard.String(), rangeStr, orderStr, orderStr)
		if withLimit {
			query += fmt.Sprintf(" LIMIT %d", limit)
		}
		fmt.Println("Query: ", query)
		var r AccountActionTraceRecord
		iterable := cs.Session.Query(query).Iter()
		for !withLimit || (limit > 0 && iterable.Scan(&r.AccountName, &r.ShardId.Time, &r.BlockTime.Time, &r.GlobalSeq, &r.Parent)) {
			records = append(records, r)
			limit -= 1
		}
		if withLimit && limit == 0 {
			break
		}
	}
	return records
}

func (cs *CassandraStorage) getLastIrreversibleBlock() (uint64, error) {
	query := fmt.Sprintf("SELECT block_num FROM %s WHERE part_key=0", TableLib)

	var lib uint64
	iterable := cs.Session.Query(query).Iter()
	if !iterable.Scan(&lib) {
		log.Println("Failed to get last irreversible block")
		return 0, errors.New("Failed to get last irreversible block") //TODO: change error message
	}
	return lib, nil
}

func (cs *CassandraStorage) getAccountShards(account string, shardRange TimestampRange, order bool, limit int64) []AccountActionTraceShardRecord {
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
	iterable := cs.Session.Query(query).Iter()
	for iterable.Scan(&r.ShardId.Time) {
		records = append(records, r)
	}
	return records
}