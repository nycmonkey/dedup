package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"encoding/json"

	"github.com/gorilla/mux"
	"github.com/nycmonkey/dedup"
)

const (
	scoreLen = 64 // length of scores (hash hex digests)
)

var (
	archiveDir string
	metaFile   string
	port       int
)

func main() {
	flag.StringVar(&archiveDir, "d", "dedup-archive", "directory where dedup archive will be stored")
	flag.StringVar(&metaFile, "m", "blob.meta.json", "file where blob metadata will be persisted")
	flag.IntVar(&port, "p", 10101, "port to listen on")
	flag.Parse()
	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt)
	repo, err := dedup.NewBlobRepo(archiveDir, metaFile)
	if err != nil {
		panic(err)
	}
	defer repo.Close()
	s := server{repo: repo}
	r := mux.NewRouter()
	r.HandleFunc(fmt.Sprintf("/blob/{score:[a-f0-9]{%d}}", scoreLen), s.GetBlob).Methods("GET", "HEAD")
	r.HandleFunc(fmt.Sprintf("/blob/{score:[a-f0-9]{%d}}", scoreLen), s.PutBlob).Methods("PUT", "POST")
	r.HandleFunc("/blob/meta", s.GetMeta).Methods("GET", "HEAD")
	addr := fmt.Sprintf(":%d", port)
	h := &http.Server{
		Addr:         addr,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		Handler:      r,
	}
	go func() {
		log.Println("Listening on http://0.0.0.0", addr)
		if err := (http.ListenAndServe(addr, r)); err != nil {
			log.Fatalln(err)
		}
	}()
	<-stop
	log.Println("Shutting down...")
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	h.Shutdown(ctx)
	fmt.Println("Server gracefully stopped")
}

type server struct {
	repo *dedup.BlobRepository
}

func scoreFromRequest(r *http.Request) (score []byte, err error) {
	vars := mux.Vars(r)
	if len(vars["score"]) != scoreLen {
		err = errors.New("Invalid score")
		return
	}
	_, err = fmt.Sscanf(vars["score"], "%x", &score)
	return
}

func (s server) GetBlob(w http.ResponseWriter, r *http.Request) {
	score, err := scoreFromRequest(r)
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("InvalidScore %x", score), http.StatusBadRequest)
		return
	}
	errCh := make(chan error, 1)
	cancelCh := make(chan struct{})
	log.Printf("SENDING %x\n", score)
	s.repo.Get(dedup.GetBlobRequest{Key: score, Dest: w, ErrCh: errCh, CancelCh: cancelCh})
	err = <-errCh
	if err != nil {
		log.Printf("ERROR SENDING %x: %s\n", score, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("SENT %x\n", score)
}

func (s server) PutBlob(w http.ResponseWriter, r *http.Request) {
	score, err := scoreFromRequest(r)
	defer r.Body.Close()
	if err != nil {
		log.Println(err)
		http.Error(w, fmt.Sprintf("InvalidScore %x", score), http.StatusBadRequest)
		return
	}
	log.Printf("RECEIVING %x\n", score)
	if err := s.repo.Put(score, r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}
	log.Printf("RECEIVED %x\n", score)
}

func (s server) GetMeta(w http.ResponseWriter, r *http.Request) {
	metaCh := make(chan dedup.BlobMetadata, 1)
	cancelCh := make(chan struct{})
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	go s.repo.DumpMetadata(metaCh, cancelCh)
	for meta := range metaCh {
		err := enc.Encode(meta)
		if err != nil {
			cancelCh <- struct{}{}
		}
		return
	}
}
