package main

import (
	"EOS-Cassandra-middleware/routes"
	"EOS-Cassandra-middleware/storage"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
)

const ConfigFilename = "config.json"

type Config struct {
	Port uint32 `json:"port"`
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

	var hs storage.IHistoryStorage
	hs = storage.NewMockedCassandraHistoryStorage(322)
	router := routes.NewRouter(hs)
	address := ":" + strconv.FormatUint(uint64(config.Port), 10)
	s := http.Server{ Addr: address, Handler: router }
	err = s.ListenAndServe()
	if err != http.ErrServerClosed {
		log.Println("s.ListenAndServe() returned error: " + err.Error())
	}
}