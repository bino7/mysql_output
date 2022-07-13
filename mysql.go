package mysql_output

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bino7/mysql_output/models"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"gorm.io/gorm"
	"time"
)

var (
	emptyDataErr = errors.New("record has no data")
)

func init() {
	outputs.RegisterType("mysql", makeMysql)
}

const (
	defaultWaitRetry = 1 * time.Second

	// NOTE: maxWaitRetry has no effect on mode, as logstash client currently does
	// not return ErrTempBulkFailure
	defaultMaxWaitRetry = 60 * time.Second

	debugSelector = "mysql"
)

var log = logp.NewLogger(debugSelector)

type record struct {
	model string
	data  []byte
	event publisher.Event
}

func (r *record) insert(db *gorm.DB) error {
	model, err := models.Get(r.model)
	if err != nil {
		return err
	}
	err = json.Unmarshal(r.data, &model)
	if err != nil {
		return fmt.Errorf("json unmarshal error:%e", err)
	}
	tx := db.Save(model)
	return tx.Error
}

func makeMysql(
	im outputs.IndexManager,
	beat beat.Info,
	stats outputs.Observer,
	cfg *common.Config) (outputs.Group, error) {
	panic("unimplemented")
}
