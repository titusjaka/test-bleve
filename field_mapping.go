package main

import (
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/analysis/analyzer/standard"
	"github.com/blevesearch/bleve/mapping"
)

func getKeywordMapping() *mapping.FieldMapping {
	keywordFieldMapping := bleve.NewTextFieldMapping()
	keywordFieldMapping.Analyzer = keyword.Name
	return keywordFieldMapping
}

func getStandardMapping() *mapping.FieldMapping {
	standardFieldMapping := bleve.NewTextFieldMapping()
	standardFieldMapping.Analyzer = standard.Name
	return standardFieldMapping
}

func getNumericMapping() *mapping.FieldMapping {
	return bleve.NewNumericFieldMapping()
}
