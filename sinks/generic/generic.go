package generic

import (
	"context"
	"net/http"

	"github.com/sirupsen/logrus"
	vhttp "github.com/stripe/veneur/http"
	"github.com/stripe/veneur/samplers"
	"github.com/stripe/veneur/sinks"
	"github.com/stripe/veneur/ssf"
	"github.com/stripe/veneur/trace"
)

// GenericMetricSink flushes batches of metrics in JSON to a configured endpoint.
type GenericMetricSink struct {
	log         *logrus.Logger
	traceClient *trace.Client
	httpClient  *http.Client
	Tags        []string
	Endpoint    string
	BatchSize   int
	Source      string
	Environment string
	Namespace   string
}

// GenericMetric represents a single metric.
type GenericMetric struct {
	Metric string            `json:"metric"`
	Value  float64           `json:"value"`
	Source string            `json:"source"`
	At     float64           `json:"at"`
	Tags   map[string]string `json:"tags"`
}

// GenericMetrics encapsulates a batch of metrics, with their common environment and namespace.
type GenericMetrics struct {
	Metrics     []GenericMetric `json:"metrics"`
	Environment string          `json:"environment"`
	Namespace   string          `json:"namespace"`
}

var _ sinks.MetricSink = &GenericMetricSink{}

// NewGenericMetricSink returns a new generic metrics sink.
func NewGenericMetricSink(
	log *logrus.Logger,
	httpClient *http.Client,
	tags []string,
	endpoint string,
	batchSize int,
	source string,
	environment string,
	namespace string,
) (*GenericMetricSink, error) {
	ret := &GenericMetricSink{
		log:         log,
		httpClient:  httpClient,
		Tags:        tags,
		Endpoint:    endpoint,
		BatchSize:   batchSize,
		Source:      source,
		Environment: environment,
		Namespace:   namespace,
	}
	return ret, nil
}

// Name returns the sink's name.
func (gm *GenericMetricSink) Name() string {
	return "generic"
}

// Start sets the trace client for the sink.
func (gm *GenericMetricSink) Start(client *trace.Client) error {
	gm.traceClient = client
	return nil
}

// Flush flushes accumulated metrics.
func (gm *GenericMetricSink) Flush(ctx context.Context, metrics []samplers.InterMetric) error {
	var batchSize int
	for len(metrics) > 0 {
		if len(metrics) > gm.BatchSize {
			batchSize = gm.BatchSize
		} else {
			batchSize = len(metrics)
		}
		batch := metrics[:batchSize]
		metrics = metrics[batchSize:]
		gm.flushBatch(batch)
	}
	return nil
}

func (gm *GenericMetricSink) flushBatch(metrics []samplers.InterMetric) {
	genMetrics := gm.convertInterToGeneric(metrics)
	err := vhttp.PostHelper(
		context.TODO(),
		gm.httpClient,
		gm.traceClient,
		http.MethodPost,
		gm.Endpoint,
		genMetrics,
		"flush_metrics",
		false,
		nil,
		gm.log,
	)
	if err == nil {
		gm.log.WithField(
			"metrics", len(metrics),
		).Info("Completed flushing generic metrics")
	} else {
		gm.log.WithFields(logrus.Fields{
			"metrics":       len(metrics),
			logrus.ErrorKey: err,
		}).Warn("Error flushing generic metrics")
	}
}

func (gm *GenericMetricSink) convertInterToGeneric(metrics []samplers.InterMetric) GenericMetrics {
	var genMetrics []GenericMetric
	for _, metric := range metrics {
		inTags := append(metric.Tags, gm.Tags...)
		outTags := samplers.ParseTagSliceToMap(inTags)
		genMetric := GenericMetric{
			Metric: metric.Name,
			Value:  metric.Value,
			Source: gm.Source,
			At:     float64(metric.Timestamp),
			Tags:   outTags,
		}
		genMetrics = append(genMetrics, genMetric)
	}
	return GenericMetrics{
		Environment: gm.Environment,
		Namespace:   gm.Namespace,
		Metrics:     genMetrics,
	}
}

// FlushOtherSamples does nothing; currently this sink only supports metrics.
func (gm *GenericMetricSink) FlushOtherSamples(ctx context.Context, samples []ssf.SSFSample) {}
