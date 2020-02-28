package main

import (
	"log"
	"testing"
)

func TestRead(t *testing.T) {
	cfg := GetConfig("config.json")
	log.Println(cfg)
}
