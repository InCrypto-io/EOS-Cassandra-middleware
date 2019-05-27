package main

import (
	"EOS-Cassandra-middleware/routes"
	"EOS-Cassandra-middleware/storage/cassandra_storage"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
)

const ConfigFilename = "config.json"

type Config struct {
	Port uint32 `json:"port"`

	CassAddress  string `json:"cassandra_address"`
	CassKeyspace string `json:"cassandra_keyspace"`
}


func main() {
	var config Config
	file, err := os.Open(ConfigFilename)
	if err != nil {
		log.Printf("Failed to open %s\n", ConfigFilename)
		return
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		log.Printf("Failed decode config\n")
		return
	}
	file.Close()

	hs, err := cassandra_storage.NewCassandraStorage(config.CassAddress, config.CassKeyspace)
	if err != nil {
		log.Println("Failed to create history storage object: " + err.Error())
		return
	}
	defer hs.Close()
	router := routes.NewRouter(hs)
	address := ":" + strconv.FormatUint(uint64(config.Port), 10)
	s := http.Server{ Addr: address, Handler: router }
	err = s.ListenAndServe()
	if err != http.ErrServerClosed {
		log.Println("s.ListenAndServe() returned error: " + err.Error())
	}
}