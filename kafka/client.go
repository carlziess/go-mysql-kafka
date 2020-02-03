package kafka

import (
	"encoding/json"
	"strconv"

	"github.com/juju/errors"
)

const (
	ActionCreate = "insert"
	ActionUpdate = "update"
	ActionDelete = "delete"
)

// Client is the client to communicate with Kafka.
// Although there are many Kafka clients with Go, I still want to implement one by myself.
// Because we only need some very simple usages.
type Client struct {
	KafkaBrokerServers string
	KafkaUserName      string
	KafkaPassword      string
}

// ClientConfig is the configuration for the client.
type ClientConfig struct {
	BrokerServers string
	UserName      string
	Password      string
}

type Request struct {
	DataBase  string
	Table     string
	Type      string
	Timestamp string
	Topic     string
	Partition int32
	Data      map[string]interface{}
	OldData   map[string]interface{}
}

type Response struct {
	Code   int
	Status string `json:"status"`
	Result bool   `json:"result"`
}

// NewClient creates the Cient with configuration.
func NewClient(conf *ClientConfig) *Client {
	c := new(Client)
	c.KafkaBrokerServers = conf.BrokerServers
	c.KafkaUserName = conf.UserName
	c.KafkaPassword = conf.Password
	return c
}

// Bulk sends the bulk request to the Kafka.
func (c *Client) Bulk(items []*Request) (*Response, error) {
	for _, item := range items {
		meta := make(map[string]interface{})
		timestamp, _ := strconv.ParseInt(item.Timestamp, 10, 64)
		meta["database"] = item.DataBase
		meta["table"] = item.Table
		meta["type"] = item.Type
		meta["ts"] = timestamp
		meta["data"] = item.Data
		if item.Type == "update" {
			meta["old"] = item.OldData
		}
		req, errs := json.Marshal(meta)
		if errs != nil {
			return nil, errors.Trace(errs)
		}
		c.send(req, item.Topic, item.Partition)
	}
	return nil, nil
}
