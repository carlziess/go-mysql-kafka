# go-mysql-kafka

The Go-MySQL-Kafka it's a MySQL bin log subscriber. And will send the message to Kafka brokers immediately. It uses [`Go-MySQL`](https://github.com/siddontang/go-mysql) to fetch the origin data.

## Install

+ Install Go (1.9+) and set your [GOPATH](https://golang.org/doc/code.html#GOPATH)
+ `go get github.com/carlziess/go-mysql-kafka`
+ cd `$GOPATH/go-mysql-kafka`
+ `make`
