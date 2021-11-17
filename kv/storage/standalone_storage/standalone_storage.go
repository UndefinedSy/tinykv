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
	bDB       *badger.DB
	bDBOption *badger.Options

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

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.logger = nil
	s.bDB.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	_tx := s.bDB.NewTransaction(false)
	reader := StandAloneStorageReader{
		storage: s,
		tx:      _tx,
	}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	return nil
}

type StandAloneStorageReader struct {
	storage *StandAloneStorage
	tx      *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {

	return nil, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return nil
}

func (r *StandAloneStorageReader) Close() {

	return
}
