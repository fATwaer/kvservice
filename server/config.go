package main

import (
	"encoding/json"
	"log"
	"os"
)

type Config struct {
	Me int			`json:"Me"`
	KVPeers []string	`json:"KVPeers"`
	RFPeers []string	`json:"RFPeers"`
}

func GetConfig(filename string) *Config {
	fp, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	cfg := &Config{}
	d := json.NewDecoder(fp)
	d.Decode(cfg)
	return cfg
}
