package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	badger "github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	bDB *badger.DB

	logger *log.Logger
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	logger := log.New()
	logger.SetLevelByString(conf.LogLevel)
	logger.SetHighlighting(true)

	db := engine_util.CreateDB(conf.DBPath, false)

	standAloneStorage := &StandAloneStorage{
		bDB: db,
		// bDBOption: &dbOption,
		logger: logger,
		// wg:        new(sync.WaitGroup),
	}

	return standAloneStorage
}

func (s *StandAloneStorage) Logger() *log.Logger {
	return s.logger
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.logger.Info("StandAloneStorage Start.")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.bDB.Close()
	s.logger.Info("StandAloneStorage Exit.")
	s.logger = nil
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	_tx := s.bDB.NewTransaction(false)
	reader := &StandAloneStorageReader{
		storage: s,
		tx:      _tx,
	}
	// s.wg.Add(1)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			wb.SetCF(put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := modify.Data.(storage.Delete)
			wb.DeleteCF(delete.Cf, delete.Key)
		}
	}
	if err := wb.WriteToDB(s.bDB); err != nil {
		s.logger.Errorf("Failed to write to db with error: %s", err)
		return err
	}
	return nil
}

type StandAloneStorageReader struct {
	storage *StandAloneStorage
	tx      *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.tx, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.tx)
}

func (r *StandAloneStorageReader) Close() {
	r.tx.Discard()
	r.storage = nil
}
