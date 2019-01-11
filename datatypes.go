package main

import (
	"math/big"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
)

type DataLine struct {
	StartIP           string `csv:"start-ip"`
	EndIP             string `csv:"end-ip"`
	Country           string `csv:"edge-two-letter-country"`
	Region            string `csv:"edge-region"`
	RegionCode        string `csv:"edge-region-code"`
	City              string `csv:"edge-city"`
	CityCode          string `csv:"edge-city-code"`
	ConnSpeed         string `csv:"edge-conn-speed"`
	ISP               string `csv:"isp-name"`
	MobileCarrier     string `csv:"mobile-carrier"`
	MobileCarrierCode string `csv:"mobile-carrier-code"`
}

// BleveInfoField is used as enum
type BleveInfoField string

func (bf BleveInfoField) String() string {
	return string(bf)
}

// ToDo: rewrite on custom type tag
// Constants used as enum
const (
	InfoID                BleveInfoField = "ID"
	InfoStartIP           BleveInfoField = "StartIP"
	InfoEndIP             BleveInfoField = "EndIP"
	InfoCountry           BleveInfoField = "Country"
	InfoRegion            BleveInfoField = "Region"
	InfoRegionCode        BleveInfoField = "RegionCode"
	InfoCity              BleveInfoField = "City"
	InfoCityCode          BleveInfoField = "CityCode"
	InfoConnSpeed         BleveInfoField = "ConnSpeed"
	InfoISP               BleveInfoField = "ISP"
	InfoMobileCarrier     BleveInfoField = "MobileCarrier"
	InfoMobileCarrierCode BleveInfoField = "MobileCarrierCode"
)

type BleveInfoObject struct {
	ID                string
	StartIP           *big.Int
	EndIP             *big.Int
	Country           string
	Region            string
	RegionCode        string
	City              string
	CityCode          string
	ConnSpeed         string
	ISP               string
	MobileCarrier     string
	MobileCarrierCode string
}

func csvLineToDataLine(csvLine []string) *DataLine {
	return &DataLine{
		StartIP:           csvLine[0],
		EndIP:             csvLine[1],
		Country:           csvLine[2],
		Region:            csvLine[3],
		RegionCode:        csvLine[4],
		City:              csvLine[5],
		CityCode:          csvLine[6],
		ConnSpeed:         csvLine[7],
		ISP:               csvLine[8],
		MobileCarrier:     csvLine[9],
		MobileCarrierCode: csvLine[10],
	}
}

func (dl *DataLine) toBleveInfoObject() *BleveInfoObject {
	return &BleveInfoObject{
		StartIP:           ipToInt(dl.StartIP),
		EndIP:             ipToInt(dl.EndIP),
		Country:           dl.Country,
		Region:            dl.Region,
		RegionCode:        dl.RegionCode,
		City:              dl.City,
		CityCode:          dl.CityCode,
		ConnSpeed:         dl.ConnSpeed,
		ISP:               dl.ISP,
		MobileCarrier:     dl.MobileCarrier,
		MobileCarrierCode: dl.MobileCarrierCode,
	}
}

func (bo *BleveInfoObject) toDataLine() *DataLine {
	return &DataLine{
		StartIP:           intToIp(bo.StartIP),
		EndIP:             intToIp(bo.EndIP),
		Country:           bo.Country,
		Region:            bo.Region,
		RegionCode:        bo.RegionCode,
		City:              bo.City,
		CityCode:          bo.CityCode,
		ConnSpeed:         bo.ConnSpeed,
		ISP:               bo.ISP,
		MobileCarrier:     bo.MobileCarrier,
		MobileCarrierCode: bo.MobileCarrierCode,
	}
}

func (BleveInfoObject) Type() string {
	return "geoip-info"
}

func (BleveInfoObject) GetDocumentMapping() *mapping.DocumentMapping {
	geoInfoMapping := bleve.NewDocumentStaticMapping()

	geoInfoMapping.AddFieldMappingsAt(InfoStartIP.String(), getNumericMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoEndIP.String(), getNumericMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoCountry.String(), getStandardMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoRegion.String(), getStandardMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoRegionCode.String(), getKeywordMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoCity.String(), getStandardMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoCityCode.String(), getKeywordMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoConnSpeed.String(), getKeywordMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoISP.String(), getKeywordMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoMobileCarrier.String(), getKeywordMapping())
	geoInfoMapping.AddFieldMappingsAt(InfoMobileCarrierCode.String(), getKeywordMapping())

	return geoInfoMapping
}
