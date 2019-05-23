package cassandra_storage

import (
	"fmt"
	"testing"
)


func Test_getLastIrreversibleBlock(t *testing.T) {
	hs, err := NewTestCassandraStorage()
	if err != nil {
		t.Error("Failed to create history storage object: " + err.Error())
		return
	}
	defer hs.Close()

	err = hs.Session.Query(fmt.Sprintf("CREATE TABLE %s (part_key int, block_num varint, PRIMARY KEY(part_key))", TableLib)).Exec()
	if err != nil {
		t.Error("Failed to create table: " + err.Error())
		return
	}

	actualLib := uint64(1)
	err = hs.Session.Query(fmt.Sprintf("INSERT INTO %s (part_key, block_num) VALUES(0, %d)", TableLib, actualLib)).Exec()
	if err != nil {
		t.Error("Failed to insert lib: " + err.Error())
		return
	}

	t.Run(fmt.Sprintf("should return %d", actualLib), func(t *testing.T) {
		lib, err := hs.getLastIrreversibleBlock()
		if err != nil {
			t.Error("getLastIrreversibleBlock failed: " + err.Error())
			return
		}
		if lib != actualLib {
			t.Error("Got: ", lib, "Expected: ", actualLib)
		}
	})

	actualLib = 100000000
	err = hs.Session.Query(fmt.Sprintf(TemplateUpdateLib, actualLib)).Exec()
	if err != nil {
		t.Error("Failed to update lib: " + err.Error())
		return
	}
	t.Run(fmt.Sprintf("should return %d", actualLib), func(t *testing.T) {
		lib, err := hs.getLastIrreversibleBlock()
		if err != nil {
			t.Error("getLastIrreversibleBlock failed: " + err.Error())
			return
		}
		if lib != actualLib {
			t.Error("Got: ", lib, "Expected: ", actualLib)
		}
	})
}