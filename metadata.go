package dedup

import (
	"time"
)

// File describes a blob as it existed on a filesystem at a point in time
// The same blob can be stored as many different files, and a given filepath
// may point to a different blob at different points in time
type File struct {
	Score    string    // key to blob (same 256-bit Blake2 hexdigest)
	Path     string    // better to use drive mapping or UNC path?  Score at path can change over time.
	Time     time.Time // New File record each time we receive it?
	Observer string    // account that read the file at the time (e.g., who sent it into the vault)
}

// DetectedContent captures metadata automatically extracted from a blob (e.g., by Apache Tika)
type DetectedContent struct {
	Score               string
	MimeType            string
	ExtractedText       string // can be further analyzed for language, topic
	ExtractedAttributes map[string]string
}

// UserMetadata captures information about a blob provided by a person or specific to a business process.
// This is the stuff that can't be automatically extracted from something like Apache Tika
type UserMetadata struct {
	Score string
	Tags  map[string]string
}
