package dedup

import (
	"hash"
	"io"
	"sync"

	boom "github.com/tylertreat/BoomFilters"
)

type BlockStorage interface {
	Hasher() hash.Hash64
	Stat(id []byte) (ok bool, err error)
	Put(id []byte, data []byte) error
	Get(id []byte, dest io.WriterAt, off int64) (n int, err error)
}

type ObjectStore interface {
	Put(source io.Reader, id uint64) error
	Get(id uint64, output io.WriteCloser) error
	Hasher() hash.Hash
}

type filesystemBlockStore struct {
	hasher hash.Hash64
	root   string
}

type kvstore struct {
	lock    *sync.RWMutex
	rootDir string
	ibf     *boom.InverseBloomFilter
}

func (s *kvstore) Put(key, block []byte) error {
	return nil
}

func (s *kvstore) Get(key []byte) (block []byte, err error) {
	return
}
