package standalone_storage

import (
	"sync"

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
	bDB       *badger.DB
	bDBOption *badger.Options

	wg *sync.WaitGroup

	logger *log.Logger
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	logger := log.New()
	logger.SetLevelByString(conf.LogLevel)
	logger.SetHighlighting(true)

	dbOption := badger.DefaultOptions
	dbOption.Dir = conf.DBPath
	dbOption.ValueDir = conf.DBPath
	db, err := badger.Open(dbOption)
	if err != nil {
		logger.Errorf("Failed to open badger db with error: %s", err)
		return nil
	}

	standAloneStorage := &StandAloneStorage{
		bDB:       db,
		bDBOption: &dbOption,
		logger:    logger,
	}

	return standAloneStorage
}

func (s *StandAloneStorage) Logger() *log.Logger {
	return s.logger
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.wg.Wait()
	s.logger = nil
	s.bDB.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// _tx := s.bDB.NewTransaction(false)
	reader := &StandAloneStorageReader{
		storage: s,
		// tx:      _tx,
	}
	s.wg.Add(1)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		if err := engine_util.PutCF(s.bDB, modify.Cf(), modify.Key(), modify.Value()); err != nil {
			s.logger.Errorf("Failed to put key[%s_%s] - value[%s] with error: %s",
				modify.Cf(), modify.Key(), modify.Value(), err)
			return err
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	storage *StandAloneStorage
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(r.storage.bDB, cf, key)
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	tx := r.storage.bDB.NewTransaction(false)
	return engine_util.NewCFIterator(cf, tx)
}

func (r *StandAloneStorageReader) Close() {
	r.storage.wg.Done()
	r.storage = nil
}
