package cluster

import (
	"encoding/json"

	"github.com/guoyyit/distributed-semaphore-go/semaphore"
	"github.com/guoyyit/distributed-semaphore-go/store"
)

// encodeEvent serializes a state-machine event for the replicated log.
func encodeEvent(e semaphore.Event) ([]byte, error) {
	return json.Marshal(e)
}

// decodeEvent parses a log payload back into a state-machine event.
func decodeEvent(data []byte) (semaphore.Event, error) {
	var e semaphore.Event
	if err := json.Unmarshal(data, &e); err != nil {
		return semaphore.Event{}, err
	}
	return e, nil
}

func toRecord(e semaphore.Event, term uint64) (store.Record, error) {
	data, err := encodeEvent(e)
	if err != nil {
		return store.Record{}, err
	}
	return store.Record{Index: e.Seq, Term: term, Data: data}, nil
}
