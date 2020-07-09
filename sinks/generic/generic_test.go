package generic

import (
	"compress/zlib"
	"context"
	"fmt"
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
	Endpoint      string
	Contains      string
	GotCalled     bool
	ThingReceived bool
	Contents      string
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
		if rt.Contains != "" {
			if strings.Contains(string(body), rt.Contains) {
				rt.ThingReceived = true
			}
		}
		rt.Contents = string(body)

		rec.Code = http.StatusOK
		rt.GotCalled = true
	}

	return rec.Result(), nil
}

func TestConvertInterToGeneric(t *testing.T) {
	gmSink := GenericMetricSink{
		Source:      "source",
		Environment: "environment",
		Namespace:   "namespace",
	}
	intMetric := samplers.InterMetric{
		Name:      "foo.bar.baz",
		Timestamp: time.Now().Unix(),
		Value:     float64(42),
		Tags:      []string{"fnord:xyzzy", "qux:quux"},
		Type:      samplers.CounterMetric,
	}
	genMetrics := gmSink.convertInterToGeneric([]samplers.InterMetric{intMetric})
	expectedTags := map[string]string{
		"fnord": "xyzzy",
		"qux":   "quux",
	}
	assert.Equal(t, fmt.Sprintf("%s", expectedTags), fmt.Sprintf("%s", genMetrics[0].Tags))
}

func TestAddServerTags(t *testing.T) {
	gmSink := GenericMetricSink{
		Tags:        []string{"snowy:plover", "plugh:bletch"},
		Source:      "source",
		Environment: "environment",
		Namespace:   "namespace",
	}
	intMetric := samplers.InterMetric{
		Name:      "foo.bar.baz",
		Timestamp: time.Now().Unix(),
		Value:     float64(42),
		Tags:      []string{"fnord:xyzzy", "qux:quux"},
		Type:      samplers.CounterMetric,
	}
	genMetrics := gmSink.convertInterToGeneric([]samplers.InterMetric{intMetric})
	expectedTags := map[string]string{
		"snowy": "plover",
		"plugh": "bletch",
		"fnord": "xyzzy",
		"qux":   "quux",
	}
	assert.Equal(t, fmt.Sprintf("%s", expectedTags), fmt.Sprintf("%s", genMetrics[0].Tags))
}

func TestFlush(t *testing.T) {
	endpoint := "/fooendpoint"
	source := "foo-source"
	environment := "bar-environment"
	namespace := "baz-namespace"

	transport := &GenericRoundTripper{
		Endpoint: endpoint,
	}
	gmSink, err := NewGenericMetricSink(
		logrus.New(),
		&http.Client{Transport: transport},
		[]string{},
		endpoint,
		1000,
		source,
		environment,
		namespace,
	)
	assert.NoError(t, err)

	ts0 := time.Date(1955, time.November, 5, 6, 0, 0, 0, time.UTC)
	ts1 := ts0.Add(1 * time.Second)
	inMetrics := []samplers.InterMetric{
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

	expectMetrics := GenericMetrics{
		Metrics: []GenericMetric{
			{
				Metric: inMetrics[0].Name,
				Value:  inMetrics[0].Value,
				Source: source,
				At:     float64(inMetrics[0].Timestamp),
				Tags: map[string]string{
					"fnord": "xyzzy",
					"qux":   "quux",
				},
			},
			{
				Metric: inMetrics[1].Name,
				Value:  inMetrics[1].Value,
				Source: source,
				At:     float64(inMetrics[1].Timestamp),
				Tags: map[string]string{
					"bletch": "frotz",
					"fax":    "fox",
				},
			},
		},
		Environment: environment,
		Namespace:   namespace,
	}
	var gotMetrics GenericMetrics

	err = gmSink.Flush(context.TODO(), inMetrics)
	assert.NoError(t, err)
	assert.Equal(t, true, transport.GotCalled, "Did not call metrics endpoint")
	err = json.Unmarshal([]byte(transport.Contents), &gotMetrics)
	assert.NoError(t, err)
	assert.Equal(t, expectMetrics, gotMetrics)
}
