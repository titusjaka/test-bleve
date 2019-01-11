package main

import (
	"flag"
	"log"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

var (
	url       = flag.String("url", "http://localhost:9200", "Elasticsearch URL")
	index     = flag.String("index", "geoip_index", "Elasticsearch index name")
	filename  = flag.String("filename", "", "Path to SCV with GEO-info")
	bulkSize  = flag.Int("bulksize", 1000, "Number of documents to collect before committing")
	useSimple = flag.Bool("use-simple", false, "if true, use dynamic mapping")
)

func main() {
	flag.Parse()
	log.SetFlags(0)

	if *url == "" {
		log.Fatal("missing url parameter")
	}
	if *index == "" {
		log.Fatal("missing index parameter")
	}
	if *filename == "" {
		log.Fatal("missing PATH to CSV-file")
	}
	if *bulkSize <= 0 {
		log.Fatal("bulk-size must be a positive number")
	}

	var geoInfoIndex *GeoInfoIndex
	var err error

	if *useSimple {
		geoInfoIndex, err = OpenOrCreateSimpleGeoInfoIndex(*index)
	} else {
		geoInfoIndex, err = OpenOrCreateGeoInfoIndex(*index)
	}

	if err != nil {
		log.Fatal(err)
	}

	csvChan := make(chan DataLine)
	bleveChan := make(chan BleveInfoObject)

	gr, ctx := errgroup.WithContext(context.Background())

	gr.Go(
		func() error {
			return geoInfoIndex.Update(ctx, *bulkSize, bleveChan)
		},
	)

	gr.Go(
		func() error {
			defer log.Println("[DEBUG] CSV channel is closed")
			defer close(csvChan)
			return readDataFromCSV(*filename, ctx, csvChan)
		},
	)

	gr.Go(
		func() error {
			defer log.Println("[DEBUG] Elastic channel is closed")
			defer close(bleveChan)
			for line := range csvChan {
				id := getIdFromIpRange(line.StartIP, line.StartIP)
				bo := line.toBleveInfoObject()
				bo.ID = id
				bleveChan <- *bo
			}
			return nil
		},
	)

	err = gr.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
