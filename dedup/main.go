package main

import (
	"context"
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

var (
	archiveDir string
	port       int
)

func main() {
	flag.StringVar(&archiveDir, "d", "dedup-archive", "directory where dedup archive will be stored")
	flag.IntVar(&port, "p", 10101, "port to listen on")
	flag.Parse()
	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt)
	repo, err := dedup.NewBlobRepo(archiveDir)
	if err != nil {
		panic(err)
	}
	defer repo.Close()
	s := server{repo: repo}
	r := mux.NewRouter()
	r.HandleFunc("/blob/{score:[a-f0-9]{64}}", s.GetBlob).Methods("GET", "HEAD")
	r.HandleFunc("/blob/{score:[a-f0-9]{64}}", s.PutBlob).Methods("PUT", "POST")
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

func (s server) GetBlob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	scoreStr := vars["score"]
	var score []byte
	_, err := fmt.Sscanf(scoreStr, "%x", &score)
	if err != nil {
		log.Println(err)
		http.Error(w, "InvalidScore "+scoreStr, http.StatusBadRequest)
		return
	}
	errCh := make(chan error, 1)
	cancelCh := make(chan struct{})
	log.Println("SENDING", scoreStr)
	s.repo.Get(dedup.GetBlobRequest{Key: score, Dest: w, ErrCh: errCh, CancelCh: cancelCh})
	err = <-errCh
	if err != nil {
		log.Println("ERROR SENDING", scoreStr, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("SENT", scoreStr)
}

func (s server) PutBlob(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	scoreStr := vars["score"]
	var score []byte
	_, err := fmt.Sscanf(scoreStr, "%x", &score)
	if err != nil {
		log.Println(err)
		http.Error(w, "InvalidScore "+scoreStr, http.StatusBadRequest)
		return
	}
	log.Println("RECEIVING", scoreStr)
	if err := s.repo.Put(score, r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}
	log.Println("RECEIVED", scoreStr)
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
