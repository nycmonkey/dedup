package dedup

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"encoding/json"

	blake2b "github.com/minio/blake2b-simd"
	"github.com/restic/chunker"
)

const (
	maxChunkSize uint = 65535 // uint16 can hold size; don't change without updating Chunk struct
)

// NewBlobRepo returns a BlobRepository backed by an in-memory metadata map and on-disk key-value chunk storage
func NewBlobRepo(pathToChunkStore string, metaFilename string) (r *BlobRepository, err error) {
	chunks, err := NewChunkRepo(pathToChunkStore)
	if err != nil {
		return
	}
	md := make(map[string][]ChunkMetadata)
	var f *os.File
	f, err = os.Open(metaFilename)
	if err == nil {
		defer f.Close()
		dec := json.NewDecoder(f)
		err = dec.Decode(&md)
		if err != nil {
			log.Fatalln("failed to load existing blob metadata:", err)
		}
	}
	return &BlobRepository{
		chunks,
		metaFilename,
		md,
		new(sync.Mutex),
	}, nil
}

// GetBlobRequest contains the required parameters to fetch a Binary Large Object ("blob")
// from a BlobRepository
type GetBlobRequest struct {
	Key      []byte
	Dest     io.Writer
	ErrCh    chan<- error
	CancelCh <-chan struct{}
}

// BlobMetadata stores the information needed to rebuild a blob from chunks, and also to compare
// overlapping data between blobs
type BlobMetadata struct {
	Score  string
	Chunks []ChunkMetadata
}

// ChunkMetadata tracks the identity and size of a single chunk of a blob. The metadata reflects
// the chunk data before any compression or encryption is applied
type ChunkMetadata struct {
	// Score is the hex-encoded 256-bit Blake2 digest of the chunk
	Score string
	// Len is the length in bytes of the chunk
	Len uint16
}

// BlobRepository stores and fetches Binary Large Objects ("blobs")
type BlobRepository struct {
	chunkRepo    *KVrepository
	metaFilename string
	md           map[string][]ChunkMetadata
	*sync.Mutex
}

// Close safely closes the underlying chunk storage
func (r *BlobRepository) Close() error {
	r.Lock()
	defer r.Unlock()
	f, err := os.Create(r.metaFilename)
	if err != nil {
		return errors.New("metdata not persisted: " + err.Error())
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	err = enc.Encode(r.md)
	if err != nil {
		return errors.New("metadata not persisted: " + err.Error())
	}
	return r.chunkRepo.Close()

}

// DumpMetadata provides a point in time dump of all scores and corresponding chunks
func (r *BlobRepository) DumpMetadata(out chan<- BlobMetadata, cancel <-chan struct{}) {
	r.Lock()
	defer r.Unlock()
	defer close(out)
	for score, chunks := range r.md {
		select {
		case <-cancel:
			return
		default:
			out <- BlobMetadata{Score: score, Chunks: chunks}
		}
	}
	return
}

// AppendMetadata loads a dump of blob metadata into the repository
func (r *BlobRepository) AppendMetadata(in <-chan BlobMetadata) {
	r.Lock()
	defer r.Unlock()
	for m := range in {
		r.md[m.Score] = m.Chunks
	}
	return
}

// Get fetches a Binary Large Object associated with a Key
func (r *BlobRepository) Get(req GetBlobRequest) {
	defer close(req.ErrCh)
	r.Lock()
	// pull scores from metadata repository
	meta, ok := r.md[fmt.Sprintf("%x", req.Key)]
	r.Unlock()
	if !ok {
		req.ErrCh <- ErrScoreNotFound{Score: req.Key, Pid: os.Getpid()}
		return
	}
	buf := make([]byte, 0, maxChunkSize)
	hash := blake2b.New256()
	w := io.MultiWriter(hash, req.Dest)
	// loop through stores, getting each one from chunk repository and sending down to req.Dest
	for _, chunk := range meta {
		select {
		case <-req.CancelCh:
			return
		default:
			log.Println("FETCHING CHUNK", chunk.Score)
			var score []byte
			_, err := fmt.Sscanf(chunk.Score, "%x", &score)
			if err != nil {
				req.ErrCh <- errors.New("reading metadata: " + err.Error())
				return
			}
			buf = buf[:0]
			buf, err = r.chunkRepo.Get(score, buf)
			if err != nil {
				req.ErrCh <- err
				return
			}
			chunkScore := blake2b.Sum256(buf)
			if !bytes.Equal(score, chunkScore[:]) {
				req.ErrCh <- fmt.Errorf("UNEXPECTED CHUNK %x", chunkScore)
				return
			}
			fmt.Println("CHUNK RETRIEVED for", chunk.Score)
			_, err = w.Write(buf)
			if err != nil {
				req.ErrCh <- err
				return
			}
		}
	}
	returnedHash := hash.Sum(nil)
	if !bytes.Equal(returnedHash, req.Key) {
		req.ErrCh <- fmt.Errorf("Expected hash value of %x, got %x", req.Key, returnedHash)
	}
	return
}

// Put stores a Binary Large Object in the repository under a key
func (r *BlobRepository) Put(key []byte, blob io.Reader) (err error) {
	r.Lock()
	_, ok := r.md[fmt.Sprintf("%x", key)]
	r.Unlock()
	if ok {
		return nil
	}
	// set up a multiwriter to copy the blob into a chunker and a hash function
	hash := blake2b.New256()
	tee := io.TeeReader(blob, hash)
	ckr := chunker.NewWithBoundaries(tee, chunker.Pol(0x3DA3358B4DC173), 2048, maxChunkSize)
	buf := make([]byte, maxChunkSize)
	var meta []ChunkMetadata
	// track the ordered list of chunk scores
	var chunkSeq int
	var chunk chunker.Chunk
	for chunk, err = ckr.Next(buf); err == nil; chunk, err = ckr.Next(buf) {
		// store each chunk in the chunk repository
		cHash := blake2b.Sum256(chunk.Data)
		meta = append(meta, ChunkMetadata{Score: fmt.Sprintf("%x", cHash), Len: uint16(len(chunk.Data))})
		fmt.Printf("blob %x %04d:%x\n", key, chunkSeq, cHash)
		if repoErr := r.chunkRepo.Put(cHash, chunk.Data); repoErr != nil {
			return repoErr
		}
		chunkSeq++
	}
	if err != io.EOF {
		// error occurred while reading blob
		return err
	}
	// confirm that the calculated blob score matches the provided key
	calculated := hash.Sum(nil)
	if !bytes.Equal(calculated, key) {
		return fmt.Errorf("calculated hash of %x did not match key of %x", calculated, key)
	}
	// commit the list of chunk scores to the metadata repository
	r.Lock()
	r.md[fmt.Sprintf("%x", key)] = meta
	r.Unlock()
	return nil
}
