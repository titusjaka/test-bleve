package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/scorch"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
)

// GeoInfoIndex is implementation of index.GeoInfoIndex interface
type GeoInfoIndex struct {
	index bleve.Index
}

func (geoIndex *GeoInfoIndex) Update(context context.Context, bulkSize int, objects chan BleveInfoObject) error {
	var total uint64
	begin := time.Now()

	batch := geoIndex.index.NewBatch()

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
				if err := geoIndex.index.Batch(batch); err != nil {
					log.Fatal(err)
					return err
				}
				batch.Reset()
			}
		}
	}

	if batch.Size() > 0 {
		if err := geoIndex.index.Batch(batch); err != nil {
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
	bleveIdx, err := openOrCreateNewBleveIndex(indexName, BuildIndexMapping(BleveInfoObject{}))
	if err != nil {
		return nil, err
	}

	newIndex := &GeoInfoIndex{
		index: bleveIdx,
	}

	return newIndex, nil
}

// CreateTriggerIndex returns GeoInfoIndex by provided mapping
func OpenOrCreateSimpleGeoInfoIndex(indexName string) (*GeoInfoIndex, error) {
	name := fmt.Sprintf("%s-simple", indexName)

	bleveIdx, err := openOrCreateNewBleveIndex(name, bleve.NewIndexMapping())
	if err != nil {
		return nil, err
	}

	newIndex := &GeoInfoIndex{
		index: bleveIdx,
	}

	return newIndex, nil
}

func openOrCreateNewBleveIndex(indexName string, indexMapping mapping.IndexMapping) (bleveIdx bleve.Index, err error) {
	bleveIdx, err = bleve.Open(indexName)

	// create new index
	if err == bleve.ErrorIndexPathDoesNotExist {

		kvStore := scorch.Name
		kvConfig := map[string]interface{}{
			"create_if_missing": true,
		}
		bleveIdx, err = bleve.NewUsing(indexName, indexMapping, "scorch", kvStore, kvConfig)
		if err != nil {
			return nil, err
		}
	}
	return
}

func buildSearchQuery(ipAddr string) query.Query {
	ipNumeric := ipToInt(ipAddr)
	q1 := bleve.NewQueryStringQuery(fmt.Sprintf("%s:>=%d", InfoStartIP, ipNumeric))
	q2 := bleve.NewQueryStringQuery(fmt.Sprintf("%s:<=%d", InfoEndIP, ipNumeric))

	return bleve.NewConjunctionQuery(q1, q2)
}
