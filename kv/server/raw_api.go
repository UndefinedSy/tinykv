package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	res := &kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	res.Value = val
	// Your Code Here (1).
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	res := &kvrpcpb.RawPutResponse{}

	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}

	batch := make([]storage.Modify, 0)
	batch = append(batch, storage.Modify{
		Data: put,
	})

	err := server.storage.Write(req.Context, batch)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	// Your Code Here (1).
	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	res := &kvrpcpb.RawDeleteResponse{}

	delete := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}

	batch := make([]storage.Modify, 0)
	batch = append(batch, storage.Modify{
		Data: delete,
	})

	err := server.storage.Write(req.Context, batch)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	// Your Code Here (1).
	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	res := &kvrpcpb.RawScanResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	for iter.Seek(req.GetStartKey()); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		val, err := item.Value()
		if err != nil {
			res.Error = err.Error()
			return res, err
		}
		kvPair := &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		res.Kvs = append(res.Kvs, kvPair)
	}

	// Your Code Here (1).
	return res, nil
}
