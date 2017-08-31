package server

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/tecbot/gorocksdb"
)

const (
	jobStateInit = iota
	jobStateProcessing
)

// Store manages everything related to RocksDB. It incldues reading and building
// the store from Kafka, reblance the store from Kafka, and reading/querying
// the store for the clients
type Store struct {
	// name is the unique name for the cluster
	name string
	// path is the parent directory for all rocksdb databases.
	path string

	db          *gorocksdb.DB
	walWriteOpt *gorocksdb.WriteOptions

	// column family names
	cf  []string
	cfh []*gorocksdb.ColumnFamilyHandle

	// read chanel from WAL
	walCh     chan *Job
	walOffset string

	// workflowCFHandleMap maps from the name of workflow to a ColumnFamily
	workflowCFHandleMap map[string]*gorocksdb.ColumnFamilyHandle
	// cfMapLock synchronizes access to workflowCFHandleMap
	workflowCFHandleMapLock sync.RWMutex
}

// NewStore creates a new RocksDB database for the cluster name
// at a location specified by path.
func NewStore(name string, path string, broker string) *Store {
	p := filepath.Join(path, name)
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

func (s *Store) open() {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	db, err := gorocksdb.OpenDb(opts, filepath.Join(s.path, s.name))
	if err != nil {
		panic(err)
	}
	s.db = db
	s.walWriteOpt = gorocksdb.NewDefaultWriteOptions()
}

// AddJob adds a job from Kafka to storage. Each workflow has its own
// column family, and the colun family is dynamically allocated.
func (s *Store) AddJob(data []byte) error {
	var job Job
	if err := proto.Unmarshal(data, &job); err != nil {
		return err
	}

	if err := s.createCFIfMissing(job.Workflow); err != nil {
		return err
	}

	s.workflowCFHandleMapLock.RLock()
	cf := s.workflowCFHandleMap[job.Workflow]
	s.workflowCFHandleMapLock.RUnlock()

	if err := s.db.PutCF(s.walWriteOpt, cf, []byte(job.Name), data); err != nil {
		return err
	}

	return nil
}

func (s *Store) createCFIfMissing(workflow string) error {
	s.workflowCFHandleMapLock.RLock()
	_, ok := s.workflowCFHandleMap[workflow]
	s.workflowCFHandleMapLock.RUnlock()

	if !ok {
		opts := gorocksdb.NewDefaultOptions()
		opts.SetCreateIfMissing(true)
		opts.SetCreateIfMissingColumnFamilies(true)

		cf, err := s.db.CreateColumnFamily(opts, workflow)
		if err != nil {
			return err
		}
		s.workflowCFHandleMapLock.Lock()
		s.workflowCFHandleMap[workflow] = cf
		s.workflowCFHandleMapLock.Unlock()
	}

	return nil
}

// AppendWAL appends a Kafka message to the WAL CF.
func (s *Store) AppendWAL(key []byte, value []byte) {
	s.db.PutCF(s.walWriteOpt, s.cfh[1], key, value)
}

// ReadWAL reads WAL by the order and send the result to
// the read channel walCh.
func (s *Store) ReadWAL() {
	ro := gorocksdb.NewDefaultReadOptions()
	iter := s.db.NewIteratorCF(ro, s.cfh[1])

	if s.walOffset == "" {
		iter.SeekToFirst()
	} else {
		iter.SeekForPrev([]byte(s.walOffset))
	}

	batchSize := 100
	current := 0
	var key []byte
	for ; iter.Valid(); iter.Next() {
		key = iter.Key().Data()
		value := iter.Value().Data()
		var job Job
		proto.Unmarshal(value, &job)
		s.walCh <- &job
		if current >= batchSize {
			current -= batchSize
			s.walOffset = string(key)
		}
	}

	s.walOffset = string(key)
}

// Close closes the storage engine
func (s *Store) Close() {
	if s.walWriteOpt != nil {
		s.walWriteOpt.Destroy()
		s.walWriteOpt = nil
	}

	s.workflowCFHandleMapLock.Lock()
	for _, cf := range s.workflowCFHandleMap {
		cf.Destroy()
	}
	s.workflowCFHandleMapLock.Unlock()

	close(s.walCh)
}
