package river

import (
	"github.com/siddontang/go-mysql/schema"
)

// Rule is the rule for how to sync data from MySQL to OpenSearch.
// If you want to sync MySQL data into OpenSearch, you must set a rule to let use know how to do it.
type Rule struct {
	Schema    string `toml:"schema"`
	Table     string `toml:"table"`
	Topic     string `toml:"topic"`
	Partition int32  `toml:"partition"`
	Index     int    `toml:"index"`
	// MySQL table information
	TableInfo *schema.Table
	//only MySQL fields in filter will be synced , default sync all fields
	Filter     []string `toml:"filter"`
	PrimaryKey string   `toml:"primary_key"`
}

func newDefaultRule(schema string, table string) *Rule {
	r := new(Rule)
	r.Schema = schema
	r.Table = table
	return r
}

func (r *Rule) prepare() error {
	return nil
}

// CheckFilter checkers whether the field needs to be filtered.
func (r *Rule) CheckFilter(field string) bool {
	if r.Filter == nil {
		return true
	}

	for _, f := range r.Filter {
		if f == field {
			return true
		}
	}
	return false
}
