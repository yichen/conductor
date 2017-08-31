package server

import (
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/proto"

	"github.com/tecbot/gorocksdb"
)

// Store is the local RocksDB storage manager
type Store struct {
	name string
	path string

	db          *gorocksdb.DB
	walWriteOpt *gorocksdb.WriteOptions

	// column family names
	cf  []string
	cfh []*gorocksdb.ColumnFamilyHandle

	// read chanel from WAL
	walCh chan *Job
}

// NewStore creates a new RocksDB database
func NewStore(name string) *Store {
	p, err := ioutil.TempDir("", "conductor")
	if err != nil {
		return nil
	}
	_ = os.Mkdir(p, os.ModePerm)

	return &Store{
		name:  name,
		cf:    []string{"default", "wal"},
		path:  p,
		walCh: make(chan *Job, 1),
	}
}

// Open creates rocksdb file
func (s *Store) Open() error {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	db, cfh, err := gorocksdb.OpenDbColumnFamilies(opts, s.path, s.cf, []*gorocksdb.Options{opts, opts})
	if err != nil {
		opts.Destroy()
		return err
	}

	s.db = db
	s.cfh = cfh
	s.walWriteOpt = gorocksdb.NewDefaultWriteOptions()

	return nil
}

// AppendWAL appends a Kafka message to the WAL CF.
func (s *Store) AppendWAL(key []byte, value []byte) {
	s.db.PutCF(s.walWriteOpt, s.cfh[1], key, value)
}

func (s *Store) ReadWAL(offset string) {
	ro := gorocksdb.NewDefaultReadOptions()
	iter := s.db.NewIteratorCF(ro, s.cfh[1])

	if offset == "" {
		iter.SeekToFirst()
	} else {
		iter.SeekForPrev([]byte(offset))
	}

	for ; iter.Valid(); iter.Next() {
		value := iter.Value().Data()
		var job Job
		proto.Unmarshal(value, &job)
		s.walCh <- &job
	}

}

// Close closes the storage engine
func (s *Store) Close() {
	if s.walWriteOpt != nil {
		s.walWriteOpt.Destroy()
		s.walWriteOpt = nil
	}

	for _, i := range s.cfh {
		i.Destroy()
	}

	close(s.walCh)
}
