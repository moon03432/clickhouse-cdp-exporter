package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/moon03432/clickhouse-cdp-exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	listeningAddress    = flag.String("telemetry.address", ":9116", "Address on which to expose metrics.")
	metricsEndpoint     = flag.String("telemetry.endpoint", "/metrics", "Path under which to expose metrics.")
	clickhouseScrapeURI = flag.String("scrape_uri", "http://localhost:8123/", "URI to clickhouse http endpoint")
	insecure            = flag.Bool("insecure", true, "Ignore server certificate if using https")
	user                = os.Getenv("CLICKHOUSE_USER")
	password            = os.Getenv("CLICKHOUSE_PASSWORD")
	tableString         = flag.String("clickhouse.tables", "", "clickhouse tables to count")
)

func main() {
	flag.Parse()

	tables := strings.Split(*tableString, ",")

	uri, err := url.Parse(*clickhouseScrapeURI)
	if err != nil {
		log.Fatal(err)
	}
	e := exporter.NewExporter(*uri, *insecure, user, password, tables)
	prometheus.MustRegister(e)

	log.Printf("Starting Server: %s", *listeningAddress)
	http.Handle(*metricsEndpoint, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Clickhouse Exporter</title></head>
			<body>
			<h1>Clickhouse Exporter</h1>
			<p><a href="` + *metricsEndpoint + `">Metrics</a></p>
			</body>
			</html>`))
	})

	log.Fatal(http.ListenAndServe(*listeningAddress, nil))
}
