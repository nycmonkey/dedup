package dedup

import (
	"os"

	"github.com/dgraph-io/badger/badger"
)

// ListRequest is sent to a storage node to find out what Scores it has
type ListRequest struct {
	Cancel  <-chan struct{}
	Results chan<- []byte
	Error   chan<- error
}

// NewChunkRepo opens or creates a keyvalue database at the specified location
func NewChunkRepo(path string) (repo *KVrepository, err error) {
	var db *badger.KV
	opt := badger.DefaultOptions
	opt.Dir = path
	db = badger.NewKV(&opt)
	return &KVrepository{db: db}, nil
}

// KVrepository stores bytes in a key-value store indexed by key. The key must be unique and immutable, e.g.,
// the sum of a strong cryptographic hash over the bytes
type KVrepository struct {
	db *badger.KV
}

// Get fetches the bytes associated with a score.  The provided buf will be reused if
// it has sufficient capacity.  Passing a nil buf is valid.
func (r *KVrepository) Get(score, buf []byte) (data []byte, err error) {
	data, _ = r.db.Get(score)
	if data == nil {
		err = ErrScoreNotFound{Score: score, Pid: os.Getpid()}
	}
	return
}

// Close closes the repository's data store
func (r *KVrepository) Close() error {
	r.db.Close()
	return nil
}

// Put sets the bytes associated with a score.  No replacement is performed if the score has already
// been associated with bytes. Scores are assumed to be unique and immutable.
func (r *KVrepository) Put(score [32]byte, chunk []byte) (err error) {
	// Put combines Get and Set in a more efficient way where the DB is searched for the key only once.
	// The upd(ater) receives the current (key, old-value), if that exists or (key, nil) otherwise.
	// It can then return a (new-value, true, nil) to create or overwrite the existing value in the KV pair,
	// or (whatever, false, nil) if it decides not to create or not to update the value of the KV pair.
	r.db.Set(score[:], chunk)
	return nil
}

// List enumerates the scores currently contained in the repository
func (r *KVrepository) List(req ListRequest) {
	// don't allow writes while this list is in progress
	// could cause DOS -- should be priveledged request that doesn't happen
	defer close(req.Results)
	defer close(req.Error)
	opt := badger.DefaultIteratorOptions
	opt.FetchValues = false
	itr := r.db.NewIterator(opt)
	for itr.Rewind(); itr.Valid(); itr.Next() {
		select {
		case <-req.Cancel:
			itr.Close()
			return
		default:
			item := itr.Item()
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())
			req.Results <- key
		}
	}
	itr.Close()
}
