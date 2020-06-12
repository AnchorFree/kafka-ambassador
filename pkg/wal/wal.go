package wal

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/anchorfree/kafka-ambassador/pkg/logger"
	"github.com/anchorfree/kafka-ambassador/pkg/wal/pb"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/ptypes"
	"github.com/imdario/mergo"
	"github.com/prometheus/client_golang/prometheus"
)

type I interface {
	Close() error
	Del([]byte) error
	Get([]byte) (*pb.Record, error)
	Iterate(int64) chan *pb.Record
	Set(string, []byte) error
	MessageCount() int64
}

type KV struct {
	k []byte
	v []byte
}

type Wal struct {
	logger        logger.Logger
	storage       *badger.DB
	dbOpts        badger.Options
	stopCh        chan bool
	writeCh       chan KV
	deleteCh      chan []byte
	flushWriteCh  chan struct{}
	flushDeleteCh chan struct{}
	lastWriteAt   time.Time
	config        Config
}

func New(conf Config, prom *prometheus.Registry, logger logger.Logger) (*Wal, error) {
	var err error
	if err := mergo.Merge(&conf, DefaultConfig); err != nil {
		logger.Panic("Could not merge config: %s", err)
	}
	logger.Debugf("wal config loaded: %+v", conf)

	// Setup BadgerDB options
	if conf.Path != "" {
		ok, err := isWritable(conf.Path)
		if !ok {
			logger.Fatalf("The WAL folder does not work due to: %s", err)
		}
	}

	var opts badger.Options
	if conf.InMemory {
		logger.Info("Running WAL with InMemory mode, everything is stored in memory")
		opts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		opts = badger.DefaultOptions(conf.Path)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		storage:       db,
		dbOpts:        opts,
		config:        conf,
		logger:        logger,
		stopCh:        make(chan bool),
		writeCh:       make(chan KV, conf.WriteChSize),
		deleteCh:      make(chan []byte, conf.DeleteChSize),
		flushWriteCh:  make(chan struct{}),
		flushDeleteCh: make(chan struct{}),
	}

	registerMetrics(prom)
	go wal.collectPeriodically(conf.CollectMetricsPeriod)
	go func(w *Wal) {
		c := 0
		var err error
		batch := w.storage.NewWriteBatch()
		flush := func() {
			err = batch.Flush()
			if err != nil {
				w.logger.Errorf("Could not read record by key due to error %s", err)
			}
			batch = w.storage.NewWriteBatch()
			c = 0
		}
		for {
			select {
			case kv := <-w.writeCh:
				w.lastWriteAt = time.Now()
				if c < w.config.WriteBatchSize {
					// WriteBatch API has changed:
					// https://github.com/dgraph-io/badger/commit/cd5884e0e8ebe92de4afa8f543a8120b40551e5f
					_ = batch.Set(kv.k, kv.v)
					c++
				} else {
					flush()
				}
			case <-time.After(w.config.WriteBatchTimeout):
				//Flush an underpacked batch if there is no updates for a while
				flush()
			case <-w.flushWriteCh:
				flush()
			}
		}
	}(wal)
	go func(w *Wal) {
		c := 0
		var err error
		for {
			err := db.Update(func(txn *badger.Txn) error {
				for {
					select {
					case k := <-w.deleteCh:
						if c < w.config.DeleteBatchSize {
							err := txn.Delete(k)
							msgDeletes.Inc()
							if err != nil {
								w.logger.Errorf("Could not delete record: %s", err)
							}
							c++
						} else {
							c = 0
							return nil
						}
					case <-time.After(w.config.DeleteBatchTimeout):
						// Exit transaction since there is no deletes for a while
						return nil
					case <-w.flushDeleteCh:
						return nil
					}
				}
				return err
			})
			if err != nil {
				w.logger.Errorf("Error while trying to delete records from WAL: %s", err)
			}
		}
	}(wal)
	go func(w *Wal) {
		for _ = range time.Tick(1 * time.Minute) {
			if time.Since(w.lastWriteAt) > w.config.LogGCIdleTime {
				w.logger.Infof("Triggering badgerdb value log garbage collection. ")
				err := w.storage.RunValueLogGC(w.config.LogGCDiscardRatio)
				if err != nil && err != badger.ErrNoRewrite {
					w.logger.Errorf("Error running badger value log GC: %s", err)
				}
			}
		}
	}(wal)
	return wal, nil
}

func (w *Wal) FlushWrites() error {
	w.flushWriteCh <- struct{}{}
	//send once again to make sure the batch has been written before calling Sync()
	w.flushWriteCh <- struct{}{}
	return w.storage.Sync()
}

func (w *Wal) FlushDeletes() error {
	w.flushDeleteCh <- struct{}{}
	//send once again to make sure the transaction has been commited before calling Sync()
	w.flushDeleteCh <- struct{}{}
	return w.storage.Sync()
}

func (w *Wal) Close() error {
	_ = w.FlushWrites()
	_ = w.FlushDeletes()

	// Close BadgerDB
	return w.storage.Close()
}

func (w *Wal) Set(topic string, value []byte) error {
	r := pb.Record{
		Timestamp: ptypes.TimestampNow(),
		Crc:       CrcSum(value),
		Payload:   value,
		Topic:     topic,
	}

	key := Uint32ToBytes(r.Crc)
	b, err := ToBytes(r)
	if err != nil {
		return err
	}
	msgWrites.With(prometheus.Labels{"topic": topic}).Inc()
	w.writeCh <- KV{key, b}
	return err
}

func (w *Wal) Get(key []byte) (*pb.Record, error) {
	var r *pb.Record
	err := w.storage.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			w.logger.Errorf("Could not read record by key due to error %s", err)
			return err
		}
		err = item.Value(func(v []byte) error {
			var err error
			r, err = FromBytes(v)
			if err != nil {
				w.logger.Errorf("Could not read value from record due to error %s", err)
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return r, err
	}

	msgReads.With(prometheus.Labels{"topic": r.Topic}).Inc()
	return r, nil
}

func (w *Wal) Del(key []byte) error {
	w.deleteCh <- key
	return nil
}

func (w *Wal) MessageCount() (cnt int64) {
	err := w.storage.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			cnt++
		}
		return nil
	})
	if err != nil {
		w.logger.Errorf("Could not count database keys")
	}
	return cnt
}

func (w *Wal) Iterate(limit int64) chan *pb.Record {
	c := make(chan *pb.Record)
	count := int64(0)

	go func(c chan *pb.Record) {
		defer close(c)
		err := w.storage.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = w.config.IteratorPrefetchSize
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid() && (count < limit || limit == 0); it.Next() {
				count++
				item := it.Item()
				err := item.Value(func(v []byte) error {
					r, err := FromBytes(v)
					if err != nil {
						w.logger.Errorf("Could not read from record due to error %s", err)
						return err
					}
					c <- r
					msgReads.With(prometheus.Labels{"topic": r.Topic}).Inc()
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			w.logger.Errorf("Could not count get iterator")
		}
	}(c)
	return c
}

func isWritable(path string) (bool, error) {
	content := []byte("temporary file's content")
	tmpfile, err := ioutil.TempFile(path, "wal-test")
	if err != nil {
		return false, err
	}
	defer os.Remove(tmpfile.Name()) // clean up
	if _, err := tmpfile.Write(content); err != nil {
		return false, err
	}
	if err := tmpfile.Close(); err != nil {
		return false, err
	}
	return true, nil
}
