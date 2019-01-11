package main

import (
	"encoding/csv"
	"golang.org/x/net/context"
	"io"
	"os"
)

func readDataFromCSV(filename string, context context.Context, dataChan chan DataLine) (err error) {
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	defer f.Close()

	cr := csv.NewReader(f)
	cr.Comma = rune(';')
	cr.Comment = rune('#')
	cr.ReuseRecord = true

	for {
		d, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		select {
		case dataChan <- *csvLineToDataLine(d):
		case <-context.Done():
			return context.Err()
		}
	}
	return nil
}
