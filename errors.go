package dedup

import (
	"fmt"
)

// ErrScoreNotFound is returned by a storage node when it does not have the bytes with the requested score
type ErrScoreNotFound struct {
	Pid   int
	Score []byte
}

func (e ErrScoreNotFound) Error() string {
	return fmt.Sprintf(
		"ErrScoreNotFound: score %02X pid %d",
		e.Score,
		e.Pid,
	)
}
