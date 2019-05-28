package cassandra_storage

import "fmt"


const (
	TestCassandraAddress  = "127.0.0.1"
	TestCassandraKeyspace = "test_eos_history"

	TemplateInsertAccountActionTrace      = "INSERT INTO account_action_trace (account_name, shard_id, global_seq, block_time) VALUES('%s', '%s', %d, '%s')"
	TemplateInsertAccountActionTraceShard = "INSERT INTO account_action_trace_shard (account_name, shard_id) VALUES('%s', '%s')"
	TemplateInsertActionTrace             = "INSERT INTO action_trace (global_seq, doc) VALUES(%d, '%s')"
	TemplateUpdateLib                     = "UPDATE lib SET block_num=%d where part_key=0"
)


type TestCassandraStorage struct {
	CassandraStorage
}

func NewTestCassandraStorage() (*TestCassandraStorage, error) {
	hs, err := NewCassandraStorage(TestCassandraAddress, "")
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", TestCassandraKeyspace)
	err = hs.Session.Query(query).Exec()
	if err != nil {
		return nil, err
	}
	err = hs.Session.Query(fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", TestCassandraKeyspace)).Exec()
	if err != nil {
		return nil, err
	}

	hs, err = NewCassandraStorage(TestCassandraAddress, TestCassandraKeyspace)
	if err != nil {
		return nil, err
	}

	return &TestCassandraStorage{ CassandraStorage: *hs }, nil
}