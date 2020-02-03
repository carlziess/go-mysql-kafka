# go-mysql-kafka
go-mysql-kafka it's a mysql binlog subscriber. and will sending message to kafka brokers immediately.
It uses [`go-mysql`](https://github.com/siddontang/go-mysql) to fetch the origin data.

## Install

+ Install Go (1.9+) and set your [GOPATH](https://golang.org/doc/code.html#GOPATH)
+ `go get github.com/carlziess/go-mysql-kafka`
+ cd `$GOPATH/go-mysql-kafka`
+ `make`
