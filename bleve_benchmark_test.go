package main

import (
	"log"
	"testing"

	"github.com/Pallinder/go-randomdata"
	"github.com/blevesearch/bleve"
)

func BenchmarkIPv4RandomTest(b *testing.B) {
	bleveIndex, err := OpenOrCreateGeoInfoIndex("geoip_index")
	if err != nil {
		log.Fatal(err)
	}
	IPs := generateIPv4(1000000)
	b.ResetTimer()
	runBenchmark(b, IPs, bleveIndex)
}

func BenchmarkIPv6RandomTest(b *testing.B) {
	bleveIndex, err := OpenOrCreateGeoInfoIndex("geoip_index")
	if err != nil {
		log.Fatal(err)
	}
	IPs := generateIPv6(1000000)
	b.ResetTimer()
	runBenchmark(b, IPs, bleveIndex)
}

func BenchmarkIPv6NonRandomTest(b *testing.B) {
	bleveIndex, err := OpenOrCreateGeoInfoIndex("geoip_index")
	if err != nil {
		log.Fatal(err)
	}
	runBenchmark(b, nonRandIPS, bleveIndex)
}

func runBenchmark(b *testing.B, ips []string, geoIndex *GeoInfoIndex) {
	var index, total, found uint64
	for i := 0; i < b.N; i++ {
		ok, err := geoIndex.searchForIP(ips[index])
		if err != nil {
			log.Fatal(err)
		}
		if ok {
			found++
		}
		total++
		index++
		if int(index) >= len(ips) {
			index = 0
		}
	}
	log.Printf("Total: %d, Found: %d\n", total, found)
}

func generateIPv6(n int) []string {
	IPs := make([]string, n)
	for i := 0; i < n; i++ {
		ipv6 := randomdata.IpV6Address()
		IPs[i] = ipv6
	}
	return IPs
}

func generateIPv4(n int) []string {
	IPs := make([]string, n)
	for i := 0; i < n; i++ {
		ipv4 := randomdata.IpV4Address()
		IPs[i] = ipv4
	}
	return IPs
}

func (geoIndex *GeoInfoIndex) searchForIP(ip string) (found bool, err error) {
	query := buildSearchQuery(ip)
	request := bleve.NewSearchRequest(query)

	response, err := geoIndex.index.Search(request)
	if err != nil {
		log.Fatal(err)
	}
	if response.Hits.Len() > 0 {
		found = true
	}
	return
}

var nonRandIPS = []string{
	"2c0f:fa48:0:0:0:0:0:fff",
	"2c0f:fa48:2:0:0:0:0:fff",
	"2c0f:fa49:0:0:0:0:0:fff",
	"2c0f:fa68:0:0:0:0:0:fff",
	"2c0f:fa69:0:0:0:0:0:fff",
	"2c0f:fa70:0:0:0:0:0:fff",
	"2c0f:fa78:0:0:0:0:0:fff",
	"2c0f:fa79:0:0:0:0:0:fff",
	"2c0f:fa80:0:0:0:0:0:fff",
	"2c0f:fa88:0:0:0:0:0:fff",
	"2c0f:fa89:0:0:0:0:0:fff",
	"2c0f:fa90:0:0:0:0:0:fff",
	"2c0f:fa98:0:0:0:0:0:fff",
	"2c0f:fa99:0:0:0:0:0:fff",
	"2c0f:faa0:0:0:0:0:0:fff",
	"2c0f:faa8:0:0:0:0:0:fff",
	"2c0f:fab0:0:0:0:0:0:fff",
	"2c0f:fac0:0:0:0:0:0:fff",
	"2c0f:fac8:0:0:0:0:0:fff",
	"2c0f:fac9:0:0:0:0:0:fff",
	"2c0f:fad8:0:0:0:0:0:fff",
	"2c0f:fad9:0:0:0:0:0:fff",
	"2c0f:fae0:0:0:0:0:0:fff",
	"2c0f:fae8:0:0:0:0:0:fff",
	"2c0f:fae9:0:0:0:0:0:fff",
	"2c0f:faf0:0:0:0:0:0:fff",
	"2c0f:faf8:0:0:0:0:0:fff",
	"2c0f:faf9:0:0:0:0:0:fff",
	"2c0f:fb08:0:0:0:0:0:fff",
	"2c0f:fb09:0:0:0:0:0:fff",
	"2c0f:fb20:0:0:0:0:0:fff",
	"2c0f:fb40:0:0:0:0:0:fff",
	"2c0f:fb48:0:0:0:0:0:fff",
	"2c0f:fb49:0:0:0:0:0:fff",
	"2c0f:fb68:0:0:0:0:0:fff",
	"2c0f:fb69:0:0:0:0:0:fff",
	"2c0f:fb70:0:0:0:0:0:fff",
	"2c0f:fb78:0:0:0:0:0:fff",
	"2c0f:fb79:0:0:0:0:0:fff",
	"2c0f:fb80:0:0:0:0:0:fff",
	"2c0f:fb88:0:0:0:0:0:fff",
	"2c0f:fb89:0:0:0:0:0:fff",
	"2c0f:fb90:0:0:0:0:0:fff",
	"2c0f:fb98:0:0:0:0:0:fff",
	"2c0f:fb99:0:0:0:0:0:fff",
	"2c0f:fba0:0:0:0:0:0:fff",
	"2c0f:fba8:0:0:0:0:0:fff",
	"2c0f:fba9:0:0:0:0:0:fff",
	"2c0f:fbb0:0:0:0:0:0:fff",
	"2c0f:fbb8:0:0:0:0:0:fff",
	"2c0f:fbb9:0:0:0:0:0:fff",
	"2c0f:fbc0:fffe:ffff:0:0:0:fff",
	"2c0f:fbc0:ffff:0:0:0:0:fff",
	"2c0f:fbc0:ffff:fffe:0:0:0:fff",
	"2c0f:fbc0:ffff:ffff:0:0:0:fff",
	"2c0f:fbc8:0:0:0:0:0:fff",
	"2c0f:fbc9:0:0:0:0:0:fff",
	"2c0f:fbd0:0:0:0:0:0:fff",
	"2c0f:fbd8:0:0:0:0:0:fff",
	"2c0f:fbd9:0:0:0:0:0:fff",
	"2c0f:fbe0:0:0:0:0:0:fff",
	"2c0f:fc20:0:0:0:0:0:fff",
	"2c0f:fc40:0:0:0:0:0:fff",
	"2c0f:fc48:0:0:0:0:0:fff",
	"2c0f:fc49:0:0:0:0:0:fff",
	"2c0f:fc62:0:0:0:0:0:fff",
	"2c0f:fc68:0:0:0:0:0:fff",
	"2c0f:fc69:0:0:0:0:0:fff",
	"2c0f:fc70:0:0:0:0:0:fff",
	"2c0f:fc78:0:0:0:0:0:fff",
	"2c0f:fc79:0:0:0:0:0:fff",
	"2c0f:fc80:0:0:0:0:0:fff",
	"2c0f:fc88:0:0:0:0:0:fff",
	"2c0f:fc88:7:0:0:0:0:fff",
	"2c0f:fc8a:0:0:0:0:0:fff",
	"2c0f:fc90:0:0:0:0:0:fff",
	"2c0f:fc98:0:0:0:0:0:fff",
	"2c0f:fc99:0:0:0:0:0:fff",
	"2c0f:fca0:0:0:0:0:0:fff",
	"2c0f:fca8:0:0:0:0:0:fff",
	"2c0f:fca9:0:0:0:0:0:fff",
	"2c0f:fcb0:0:0:0:0:0:fff",
	"2c0f:fcb8:0:0:0:0:0:fff",
	"2c0f:fcb9:0:0:0:0:0:fff",
	"2c0f:fcc8:0:0:0:0:0:fff",
	"2c0f:fcc9:0:0:0:0:0:fff",
	"2c0f:fcd0:0:0:0:0:0:fff",
	"2c0f:fcd8:0:0:0:0:0:fff",
	"2c0f:fcd9:0:0:0:0:0:fff",
	"2c0f:fce0:0:0:0:0:0:fff",
	"2c0f:fce8:0:0:0:0:0:fff",
	"2c0f:fce9:0:0:0:0:0:fff",
	"2c0f:fcf0:0:0:0:0:0:fff",
	"2c0f:fcf8:0:0:0:0:0:fff",
	"2c0f:fcf9:0:0:0:0:0:fff",
	"2c0f:fd08:0:0:0:0:0:fff",
	"2c0f:fd09:0:0:0:0:0:fff",
	"2c0f:fd20:0:0:0:0:0:fff",
	"2c0f:fd28:0:0:0:0:0:fff",
	"2c0f:fd29:0:0:0:0:0:fff",
	"2c0f:fd40:0:0:0:0:0:fff",
	"2c0f:fd48:0:0:0:0:0:fff",
	"2c0f:fd49:0:0:0:0:0:fff",
	"2c0f:fd68:0:0:0:0:0:fff",
	"2c0f:fd69:0:0:0:0:0:fff",
	"2c0f:fd78:0:0:0:0:0:fff",
	"2c0f:fd79:0:0:0:0:0:fff",
	"2c0f:fd80:0:0:0:0:0:fff",
	"2c0f:fd88:0:0:0:0:0:fff",
	"2c0f:fd89:0:0:0:0:0:fff",
	"2c0f:fd90:0:0:0:0:0:fff",
	"2c0f:fd98:0:0:0:0:0:fff",
	"2c0f:fd99:0:0:0:0:0:fff",
	"2c0f:fda0:0:0:0:0:0:fff",
	"2c0f:fda8:0:0:0:0:0:fff",
	"2c0f:fda9:0:0:0:0:0:fff",
	"2c0f:fdb0:0:0:0:0:0:fff",
	"2c0f:fdb8:0:0:0:0:0:fff",
	"2c0f:fdc0:0:0:0:0:0:fff",
	"2c0f:fdc8:0:0:0:0:0:fff",
	"2c0f:fdc9:0:0:0:0:0:fff",
	"2c0f:fdd0:0:0:0:0:0:fff",
	"2c0f:fdd8:0:0:0:0:0:fff",
	"2c0f:fdd9:0:0:0:0:0:fff",
	"2c0f:fde8:0:0:0:0:0:fff",
	"2c0f:fde9:0:0:0:0:0:fff",
	"2c0f:fdf0:0:0:0:0:0:fff",
	"2c0f:fdf8:0:0:0:0:0:fff",
	"2c0f:fdf9:0:0:0:0:0:fff",
	"2c0f:fe08:0:0:0:0:0:fff",
	"2c0f:fe09:0:0:0:0:0:fff",
	"2c0f:fe20:0:0:0:0:0:fff",
	"2c0f:fe28:0:0:0:0:0:fff",
	"2c0f:fe29:0:0:0:0:0:fff",
	"2c0f:fe40:0:0:0:0:0:fff",
	"2c0f:fe68:0:0:0:0:0:fff",
	"2c0f:fe69:0:0:0:0:0:fff",
	"2c0f:fe70:0:0:0:0:0:fff",
	"2c0f:fe78:0:0:0:0:0:fff",
	"2c0f:fe79:0:0:0:0:0:fff",
	"2c0f:fe80:0:0:0:0:0:fff",
	"2c0f:fe88:0:0:0:0:0:fff",
	"2c0f:fe89:0:0:0:0:0:fff",
	"2c0f:fe90:0:0:0:0:0:fff",
	"2c0f:fe98:0:0:0:0:0:fff",
	"2c0f:fe99:0:0:0:0:0:fff",
	"2c0f:fea0:0:0:0:0:0:fff",
	"2c0f:fea8:0:0:0:0:0:fff",
	"2c0f:fea9:0:0:0:0:0:fff",
	"2c0f:feb0:2:0:0:0:0:fff",
	"2c0f:feb0:4:0:0:0:0:fff",
	"2c0f:feb0:6:0:0:0:0:fff",
	"2c0f:feb0:7:0:0:0:0:fff",
	"2c0f:feb0:8:0:0:0:0:fff",
	"2c0f:feb0:9:0:0:0:0:fff",
	"2c0f:feb0:a:0:0:0:0:fff",
	"2c0f:feb0:d:0:0:0:0:fff",
	"2c0f:feb0:e:0:0:0:0:fff",
	"2c0f:feb0:f:0:0:0:0:fff",
	"2c0f:feb0:22:0:0:0:0:fff",
	"2c0f:feb0:24:0:0:0:0:fff",
	"2c0f:feb0:26:0:0:0:0:fff",
	"2c0f:feb0:27:0:0:0:0:fff",
	"2c0f:feb0:28:0:0:0:0:fff",
	"2c0f:feb0:29:0:0:0:0:fff",
	"2c0f:feb0:2c:0:0:0:0:fff",
	"2c0f:feb0:2d:0:0:0:0:fff",
	"2c0f:feb8:0:0:0:0:0:fff",
}
