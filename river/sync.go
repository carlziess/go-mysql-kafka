package river

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"zlx.io/go-mysql-kafka/kafka"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

const mysqlDateFormat = "2006-01-02"

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r *River
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	h.r.syncCh <- posSaver{pos, true}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema, table string) error {
	err := h.r.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
		return errors.Trace(err)
	}
	return nil
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.r.syncCh <- posSaver{nextPos, true}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.r.syncCh <- posSaver{nextPos, false}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}
	var reqs []*kafka.Request
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = h.r.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = h.r.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = h.r.makeUpdateRequest(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		return errors.Errorf("make %s OpenSearch request err %v, close sync", e.Action, err)
	}

	h.r.syncCh <- reqs

	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	return nil
}

func (h *eventHandler) String() string {
	return "KafkaRiverEventHandler"
}

func (r *River) syncLoop() {
	bulkSize := r.c.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := r.c.FlushBulkTime.Duration
	if interval == 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	lastSavedTime := time.Now()
	reqs := make([]*kafka.Request, 0, 1024)

	var pos mysql.Position

	for {
		needFlush := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case []*kafka.Request:
				reqs = append(reqs, v...)
				needFlush = len(reqs) >= bulkSize
			}
		case <-ticker.C:
			needFlush = true
		case <-r.ctx.Done():
			return
		}

		if needFlush {
			// TODO: retry some times?
			if err := r.doBulk(reqs); err != nil {
				log.Errorf("do kafka bulk err %v, close sync", err)
				r.cancel()
				return
			}
			reqs = reqs[0:0]
		}

		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				r.cancel()
				return
			}
		}
	}
}

// for insert\delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*kafka.Request, error) {
	reqs := make([]*kafka.Request, 0, len(rows))
	for _, values := range rows {
		req := &kafka.Request{Timestamp: strconv.FormatInt(time.Now().UnixNano()/1e6, 10)}
		req.Topic, req.Partition = r.getKafkaConfig(rule.Schema)
		req.DataBase = rule.Schema
		req.Table = rule.Table
		if action == canal.DeleteAction {
			req.Type = kafka.ActionDelete
			r.makeInsertReqData(req, rule, values)
			r.st.DeleteNum.Add(1)
		}
		if action == canal.InsertAction {
			req.Type = kafka.ActionCreate
			r.makeInsertReqData(req, rule, values)
			r.st.InsertNum.Add(1)
		}
		reqs = append(reqs, req)
	}
	return reqs, nil
}

func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*kafka.Request, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*kafka.Request, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*kafka.Request, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}
	reqs := make([]*kafka.Request, 0, len(rows))
	for i := 0; i < len(rows); i += 2 {
		req := &kafka.Request{Timestamp: strconv.FormatInt(time.Now().UnixNano()/1e6, 10)}
		r.makeUpdateReqData(req, rule, rows[i], rows[i+1])
		reqs = append(reqs, req)
		r.st.UpdateNum.Add(1)
	}
	return reqs, nil
}

func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(time.RFC3339)
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(mysqlDateFormat, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(mysqlDateFormat)
		}
	}

	return value
}

func (r *River) getFieldParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	kafka := composedField[0]
	fieldType := ""

	if 0 == len(kafka) {
		kafka = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, kafka, fieldType
}

func (r *River) makeInsertReqData(req *kafka.Request, rule *Rule, values []interface{}) {
	req.Data = make(map[string]interface{}, len(values))
	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			continue
		}
		req.Data[c.Name] = r.makeReqColumnData(&c, values[i])
	}
}

func (r *River) makeUpdateReqData(req *kafka.Request, rule *Rule, beforeValues []interface{}, afterValues []interface{}) {
	req.Data = make(map[string]interface{}, len(afterValues))
	req.OldData = make(map[string]interface{}, len(beforeValues))
	req.DataBase = rule.Schema
	req.Table = rule.Table
	req.Type = kafka.ActionUpdate
	req.Topic, req.Partition = r.getKafkaConfig(rule.Schema)
	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) && c.Name != rule.PrimaryKey {
			continue
		}
		req.Data[c.Name] = r.makeReqColumnData(&c, afterValues[i])
		if false == reflect.DeepEqual(beforeValues[i], afterValues[i]) {
			req.OldData[c.Name] = r.makeReqColumnData(&c, beforeValues[i])
		}
	}
}

func (r *River) getKafkaConfig(db string) (string, int32) {
	if "" == db || len(r.c.Sources) == 0 {
		log.Errorf("database config not found")
		return "", 0
	}
	for i, _ := range r.c.Sources {
		if db == r.c.Sources[i].Schema {
			return r.c.Sources[i].Topic, r.c.Sources[i].Partition
		}
	}
	log.Errorf("no topic found from configfie")
	return "", 0
}

func (r *River) doBulk(reqs []*kafka.Request) error {
	if len(reqs) == 0 {
		return nil
	}
	if resp, err := r.kafka.Bulk(reqs); err != nil {
		log.Errorf("sync data err %v after binlog %s, resp %v", err, r.canal.SyncedPosition(), resp)
		return errors.Trace(err)
	}
	return nil
}
