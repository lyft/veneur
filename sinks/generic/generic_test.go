package generic

import (
	"compress/zlib"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stripe/veneur/samplers"
	"k8s.io/apimachinery/pkg/util/json"
)

type GenericRoundTripper struct {
	Endpoint string
	Called   int
	Contents []string
}

func (rt *GenericRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	if strings.HasPrefix(req.URL.Path, rt.Endpoint) {
		bstream := req.Body
		if req.Header.Get("Content-Encoding") == "deflate" {
			bstream, _ = zlib.NewReader(req.Body)
		}
		body, _ := ioutil.ReadAll(bstream)
		defer bstream.Close()
		rt.Called++
		rt.Contents = append(rt.Contents, string(body))
		rec.Code = http.StatusOK
	}

	return rec.Result(), nil
}

func basicInterMetrics() []samplers.InterMetric {
	ts0 := time.Date(1955, time.November, 5, 6, 0, 0, 0, time.UTC)
	ts1 := ts0.Add(1 * time.Second)
	interMetrics := []samplers.InterMetric{
		{
			Name:      "counter.foo",
			Timestamp: ts0.Unix(),
			Value:     float64(42),
			Tags:      []string{"fnord:xyzzy", "qux:quux"},
			Type:      samplers.CounterMetric,
		},
		{
			Name:      "gauge.bar",
			Timestamp: ts1.Unix(),
			Value:     float64(42),
			Tags:      []string{"bletch:frotz", "fax:fox"},
			Type:      samplers.GaugeMetric,
		},
	}
	return interMetrics
}

func getInterMetricsMany(n int) []samplers.InterMetric {
	interMetrics := basicInterMetrics()
	var interMetricsMany []samplers.InterMetric
	for i := 0; i < n; i++ {
		interMetricsMany = append(interMetricsMany, interMetrics[i%len(interMetrics)])
	}
	return interMetricsMany
}

func getExpectedGenericMetrics(
	source string,
	environment string,
	namespace string,
	serverTags []string,
	interMetrics []samplers.InterMetric,
) GenericMetrics {
	genericMetrics := GenericMetrics{
		Environment: environment,
		Namespace:   namespace,
	}
	for _, metric := range interMetrics {
		tags := append(metric.Tags, serverTags...)
		genMetric := GenericMetric{
			Metric: metric.Name,
			Value:  metric.Value,
			Source: source,
			At:     float64(metric.Timestamp),
			Tags:   samplers.ParseTagSliceToMap(tags),
		}
		genericMetrics.Metrics = append(genericMetrics.Metrics, genMetric)
	}
	return genericMetrics
}

func getTestSink(
	httpClient *http.Client,
	tags []string,
	endpoint string,
	batchSize int,
	source string,
	environment string,
	namespace string,
) *GenericMetricSink {
	return &GenericMetricSink{
		log:         logrus.New(),
		httpClient:  httpClient,
		Tags:        tags,
		Endpoint:    endpoint,
		BatchSize:   batchSize,
		Source:      source,
		Environment: environment,
		Namespace:   namespace,
	}
}

const (
	defaultSource      = "source"
	defaultEnvironment = "environment"
	defaultNamespace   = "namespace"
)

func defaultTestSink() *GenericMetricSink {
	return getTestSink(
		nil,
		[]string{},
		"",
		10,
		defaultSource,
		defaultEnvironment,
		defaultNamespace,
	)
}

func getRoundTripTestSink(endpoint string, batchSize int) (*GenericMetricSink, *GenericRoundTripper) {
	transport := &GenericRoundTripper{
		Endpoint: endpoint,
	}
	sink := getTestSink(
		&http.Client{Transport: transport},
		[]string{},
		endpoint,
		batchSize,
		defaultSource,
		defaultEnvironment,
		defaultNamespace,
	)
	return sink, transport
}

func TestConvertInterToGeneric(t *testing.T) {
	gmSink := defaultTestSink()
	interMetrics := []samplers.InterMetric{
		{
			Name:      "foo.bar.baz",
			Timestamp: time.Now().Unix(),
			Value:     float64(42),
			Tags:      []string{"fnord:xyzzy", "qux:quux"},
			Type:      samplers.CounterMetric,
		},
	}
	expected := getExpectedGenericMetrics(defaultSource, defaultEnvironment, defaultNamespace, []string{}, interMetrics)
	genericMetrics := gmSink.convertInterToGeneric(interMetrics)
	assert.Equal(t, expected, genericMetrics)
}

func TestAddServerTags(t *testing.T) {
	serverTags := []string{"snowy:plover", "plugh:bletch"}
	gmSink := getTestSink(
		nil,
		serverTags,
		"",
		10,
		defaultSource,
		defaultEnvironment,
		defaultNamespace,
	)
	interMetrics := basicInterMetrics()
	expected := getExpectedGenericMetrics(defaultSource, defaultEnvironment, defaultNamespace, serverTags, interMetrics)
	genericMetrics := gmSink.convertInterToGeneric(interMetrics)
	assert.Equal(t, expected, genericMetrics)
}

func TestFlush(t *testing.T) {
	gmSink, transport := getRoundTripTestSink("/endpoint", 10)

	var gotMetrics GenericMetrics
	interMetrics := basicInterMetrics()
	expected := getExpectedGenericMetrics(defaultSource, defaultEnvironment, defaultNamespace, []string{}, interMetrics)

	err := gmSink.Flush(context.TODO(), interMetrics)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.Called)
	err = json.Unmarshal([]byte(transport.Contents[0]), &gotMetrics)
	assert.NoError(t, err)
	assert.Equal(t, expected, gotMetrics)
}

func TestFlushBatch(t *testing.T) {
	gmSink, transport := getRoundTripTestSink("/endpoint", 5)

	interMetrics := getInterMetricsMany(10)
	expected := []GenericMetrics{
		getExpectedGenericMetrics(defaultSource, defaultEnvironment, defaultNamespace, []string{}, interMetrics[:5]),
		getExpectedGenericMetrics(defaultSource, defaultEnvironment, defaultNamespace, []string{}, interMetrics[5:]),
	}

	err := gmSink.Flush(context.TODO(), interMetrics)
	assert.NoError(t, err)
	assert.Equal(t, 2, transport.Called)
	for i := 0; i < 2; i++ {
		var gotMetrics GenericMetrics
		err = json.Unmarshal([]byte(transport.Contents[i]), &gotMetrics)
		assert.NoError(t, err)
		assert.Equal(t, expected[i], gotMetrics)
	}
}
