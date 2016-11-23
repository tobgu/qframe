package main

import (
	"github.com/tobgu/go-qcache/qcache"
	"log"
	"net/http"
)

func main() {
	log.Fatal(http.ListenAndServe(":8888", qcache.Application()))
}
