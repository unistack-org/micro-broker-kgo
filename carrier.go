package kgo

import (
	"net/http"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v4/metadata"
)

// RecordCarrier injects and extracts traces from a kgo.Record.
//
// This type exists to satisfy the otel/propagation.TextMapCarrier interface.
type RecordCarrier struct {
	record *kgo.Record
}

// NewRecordCarrier creates a new RecordCarrier.
func NewRecordCarrier(record *kgo.Record) RecordCarrier {
	return RecordCarrier{record: record}
}

// Get retrieves a single value for a given key if it exists.
func (c RecordCarrier) Get(key string) string {
	for _, h := range c.record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set sets a header.
func (c RecordCarrier) Set(key, val string) {
	// Check if key already exists.
	for i, h := range c.record.Headers {
		if h.Key == key {
			// Key exist, update the value.
			c.record.Headers[i].Value = []byte(val)
			return
		}
	}
	// Key does not exist, append new header.
	c.record.Headers = append(c.record.Headers, kgo.RecordHeader{
		Key:   key,
		Value: []byte(val),
	})
}

// Keys returns a slice of all key identifiers in the carrier.
func (c RecordCarrier) Keys() []string {
	out := make([]string, len(c.record.Headers))
	for i, h := range c.record.Headers {
		out[i] = h.Key
	}
	return out
}

func setHeaders(r *kgo.Record, md metadata.Metadata, exclude ...string) {
	seen := make(map[string]struct{})

loop:
	for k, v := range md {
		k = http.CanonicalHeaderKey(k)

		if _, ok := seen[k]; ok {
			continue loop
		}

		if slices.ContainsFunc(exclude, func(s string) bool {
			return strings.EqualFold(s, k)
		}) {
			continue loop
		}

		for i := 0; i < len(r.Headers); i++ {
			if strings.EqualFold(r.Headers[i].Key, k) {
				// Key exist, update the value.
				r.Headers[i].Value = []byte(strings.Join(v, ","))
				continue loop
			}
		}

		// Key does not exist, append new header.
		r.Headers = append(r.Headers, kgo.RecordHeader{
			Key:   k,
			Value: []byte(strings.Join(v, ",")),
		})

		seen[k] = struct{}{}
	}
}
