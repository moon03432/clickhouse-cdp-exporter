package exporter

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "clickhouse" // For Prometheus metrics.
)

// Exporter collects clickhouse stats from the given URI and exports them using
// the prometheus metrics package.
type Exporter struct {
	uri             url.URL
	tables          []string
	client          *http.Client

	scrapeFailures prometheus.Counter

	user     string
	password string
}

// NewExporter returns an initialized Exporter.
func NewExporter(uri url.URL, insecure bool, user, password string, tables []string) *Exporter {
	
	return &Exporter{
		uri:    uri,
		tables: tables,
		scrapeFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrape_failures_total",
			Help:      "Number of errors while scraping clickhouse.",
		}),
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
			},
			Timeout: 30 * time.Second,
		},
		user:     user,
		password: password,
	}
}

// Describe describes all the metrics ever exported by the clickhouse exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// from clickhouse. So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {

	for _,table := range e.tables {
		uri := e.generateQuery(table)
		count, err := e.parseCountResponse(uri)
		if err != nil {
			return fmt.Errorf("Error counting table %v: %v", table, err)
		}
		newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      table,
			Help:      "Number of table " + table,
		}, []string{}).WithLabelValues()
		newMetric.Set(float64(count))
		newMetric.Collect(ch)
	}

	return nil
}

func (e *Exporter) handleResponse(uri string) ([]byte, error) {
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}
	if e.user != "" && e.password != "" {
		req.Header.Set("X-ClickHouse-User", e.user)
		req.Header.Set("X-ClickHouse-Key", e.password)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Error scraping clickhouse: %v", err)
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		if err != nil {
			data = []byte(err.Error())
		}
		return nil, fmt.Errorf("Status %s (%d): %s", resp.Status, resp.StatusCode, data)
	}
	
	return data, nil
}

func (e *Exporter) parseCountResponse(uri string) (uint64, error) {
	data, err := e.handleResponse(uri)
	if err != nil {
		return 0, err
	}

	// Parsing results
	res := string(data)
	res = strings.TrimSuffix(res, "\n")
	count, err := strconv.ParseUint(res, 10, 64)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (e *Exporter) generateQuery(table string) string {
	q := e.uri.Query()
	tableUri := e.uri
	q.Set("query", "select count(*) from " + table)
	tableUri.RawQuery = q.Encode()
	return tableUri.String()
}

// Collect fetches the stats from configured clickhouse location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	if err := e.collect(ch); err != nil {
		log.Printf("Error scraping clickhouse: %s", err)
		e.scrapeFailures.Inc()
		e.scrapeFailures.Collect(ch)
	}
}

// check interface
var _ prometheus.Collector = (*Exporter)(nil)
