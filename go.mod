module zlx.io/go-mysql-kafka

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/juju/errors v0.0.0-20190207033735-e65537c515d7
	github.com/pingcap/errors v0.11.0
	github.com/pingcap/parser v0.0.0-20190506092653-e336082eb825
	github.com/satori/go.uuid v1.2.0
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07
	github.com/siddontang/go-mysql v0.0.0-20190303113352-670f74e8daf5
	gopkg.in/confluentinc/confluent-kafka-go.v1 v1.1.0
)

replace zlx.io/go-mysql-kafka v0.0.0-20191028234829-f9344640 => ./

replace github.com/siddontang/go-mysql v0.0.0-20190303113352-670f74e8daf5 => ./go-mysql
