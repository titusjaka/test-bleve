package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve"
)

// GeoInfoIndex is implementation of index.GeoInfoIndex interface
type GeoInfoIndex struct {
	index bleve.Index
}

func (infoIndex *GeoInfoIndex) Update(context context.Context, bulkSize int, objects chan BleveInfoObject) error {
	var total uint64
	begin := time.Now()

	batch := infoIndex.index.NewBatch()

	for obj := range objects {
		current := atomic.AddUint64(&total, 1)
		duration := time.Since(begin).Seconds()
		seconds := int(duration)
		pps := int64(float64(current) / duration)
		fmt.Printf("%10d | %6d req/s | %02d:%02d\r", current, pps, seconds/60, seconds%60)

		select {
		case <-context.Done():
			log.Println("[DEBUG] Context is done")
			return context.Err()
		default:
			if err := batch.Index(obj.ID, obj); err != nil {
				log.Fatal(err)
				return err
			}
			// commit
			if batch.Size() >= bulkSize {
				if err := infoIndex.index.Batch(batch); err != nil {
					log.Fatal(err)
					return err
				}
				batch.Reset()
			}
		}
	}

	if batch.Size() > 0 {
		if err := infoIndex.index.Batch(batch); err != nil {
			log.Fatal(err)
			return err
		}
		batch.Reset()
	}

	// Final results
	dur := time.Since(begin).Seconds()
	sec := int(dur)
	pps := int64(float64(total) / dur)
	fmt.Printf("%10d | %6d req/s | %02d:%02d\n", total, pps, sec/60, sec%60)
	return nil
}

// CreateTriggerIndex returns GeoInfoIndex by provided mapping
func OpenOrCreateGeoInfoIndex(indexName string) (*GeoInfoIndex, error) {
	bleveIdx, err := bleve.Open(indexName)

	// create new index
	if err != nil {
		indexMapping := BuildIndexMapping(BleveInfoObject{})
		bleveIdx, err = bleve.New(indexName, indexMapping)
		if err != nil {
			return nil, err
		}
	}

	newIndex := &GeoInfoIndex{
		index: bleveIdx,
	}

	return newIndex, nil
}

// CreateTriggerIndex returns GeoInfoIndex by provided mapping
func OpenOrCreateSimpleGeoInfoIndex(indexName string) (*GeoInfoIndex, error) {
	name := fmt.Sprintf("%s-simple", indexName)
	bleveIdx, err := bleve.Open(name)

	// create new index
	if err != nil {
		indexMapping := bleve.NewIndexMapping()
		bleveIdx, err = bleve.New(name, indexMapping)
		if err != nil {
			return nil, err
		}
	}

	newIndex := &GeoInfoIndex{
		index: bleveIdx,
	}

	return newIndex, nil
}
