package mysql_output

import (
	"errors"
	"fmt"
	"github.com/bino7/mysql_output/models"
	"github.com/elastic/beats/v7/libbeat/common/fmtstr"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/outil"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sync"
)

var errNoNameSelected = errors.New("no model model could be selected")

type client struct {
	db       *gorm.DB
	mux      sync.Mutex
	observer outputs.Observer
	key      *fmtstr.EventFormatString
	topic    outil.Selector
	cfg      *mysql.Config
}

func (c *client) Close() error {
	c.mux.Lock()
	defer c.mux.Unlock()
	log.Debugf("closed mysql client")
	return nil
}

func (c *client) Publish(publisherBatch publisher.Batch) error {
	events := publisherBatch.Events()
	batch := &batch{
		Batch:  publisherBatch,
		client: c,
		count:  int32(len(events)),
		total:  len(events),
		failed: nil,
	}
	c.observer.NewBatch(len(events))
	for _, d := range events {
		r, err := c.getRecord(&d)
		if err != nil {
			switch err {
			case models.ModelNotFoundErr:
				log.Error("Mysql : dropping invalid message,(model=%v) not found", r.model)
			case emptyDataErr:
				log.Error("Mysql : dropping invalid message,(model=%v) data is empty", r.model)
			default:
				log.Error("Dropping event: %v", err)
			}
			batch.done()
			c.observer.Dropped(1)
			return err
		}
		err = r.insert(c.db)
		if err != nil {
			batch.fail(r, err)
			c.observer.Failed(1)
		}
		batch.done()
	}
	return nil
}

func (c *client) String() string {
	return "mysql(" + c.cfg.DSN + ")"
}

func (c *client) Connect() error {
	db, err := gorm.Open(mysql.Open(c.cfg.DSN), &gorm.Config{})
	if err != nil {
		return err
	}
	c.db = db
	return nil
}

func (c *client) getRecord(d *publisher.Event) (r *record, err error) {
	r = &record{
		event: *d,
	}
	event := &d.Content
	value, err := d.Cache.GetValue("model")
	if err == nil {
		if name, ok := value.(string); ok {
			r.model = name
		}
	}
	if r.model == "" {
		model, err := c.topic.Select(event)
		if err != nil {
			err = fmt.Errorf("find mysql model name failed with %v", err)
			return
		}
		if model == "" {
			err = errNoNameSelected
			return
		}
		r.model = model
		if _, err := d.Cache.Put("model", model); err != nil {
			err = fmt.Errorf("setting mysql model name in publisher event failed: %v", err)
			return
		}
	}

	return
}

func newMysqlClient(
	observer outputs.Observer,
	key *fmtstr.EventFormatString,
	topic outil.Selector,
	cfg *mysql.Config) (*client, error) {
	return &client{
		observer: observer,
		key:      key,
		topic:    topic,
		cfg:      cfg,
	}, nil
}
