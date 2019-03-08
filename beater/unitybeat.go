package beater

import (
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/kckecheng/storagemetric/dell/emc/unity"
	"github.com/kckecheng/unitybeat/config"
	"time"
)

// Unitybeat configuration.
type Unitybeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client
	box    *unity.Unity // Unity Connections
	qid    []int        // Metric Query IDs
}

// New creates an instance of unitybeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Unitybeat{
		done:   make(chan struct{}),
		config: c,
	}

	box, err := newUnity(bt)
	if err != nil {
		return nil, err
	}
	bt.box = box

	interval := int(bt.config.Period / time.Second)
	// Unity realtime metric query works with the mechanism that if a query result
	// is not checked within 10 x interval seconds, the query will be deleted.
	// This makes the query expires easily if it is set to a small value. In the
	// meanwhile, if the interval is larger than 60 seconds, historical metric is
	// supposed to be used.
	if interval < 10 || interval > 60 {
		return nil, fmt.Errorf("The configured period %d must between 10 and 60", interval)
	}
	ids, err := newQuery(box, interval)
	if err != nil {
		return nil, err
	}
	bt.qid = ids

	return bt, nil
}

// Run starts unitybeat.
func (bt *Unitybeat) Run(b *beat.Beat) error {
	logp.Info("unitybeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(bt.config.Period)

	// Init baseline
	interval := float64(bt.config.Period / time.Second)
	logp.Info("Sleep %v seconds to get metric baseline for the first time", interval)
	<-ticker.C
	spMetricsBaseline := extractSPMetrics(getMetrics(bt.box, bt.qid))
	iopsBaseline := extractFeature(spMetricsBaseline, "sp.*.storage.summary.reads", "sp.*.storage.summary.writes")
	bandwidthBaseline := extractFeature(spMetricsBaseline, "sp.*.storage.summary.readBlocks", "sp.*.storage.summary.writeBlocks")

	// Normal work
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		metrics := getMetrics(bt.box, bt.qid)
		event := beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":  "unitybeat",
				"unity": bt.config.Host,
			},
		}

		spMetrics := extractSPMetrics(metrics)

		var feature map[string]float64
		var spaV, spbV float64

		feature = extractFeature(spMetrics, "sp.*.cpu.summary.busyTicks", "sp.*.cpu.summary.idleTicks")
		spaV = getAverage(feature["spaR"]*100, 0, feature["spaT"])
		spbV = getAverage(feature["spbR"]*100, 0, feature["spbT"])
		updateEvent(&event, "sputilization", "Storage Processor Utlization(%)", spaV, spbV, false)

		feature = extractFeature(spMetrics, "sp.*.storage.summary.reads", "sp.*.storage.summary.writes")
		spaV = getAverage(feature["spaT"], iopsBaseline["spaT"], interval)
		spbV = getAverage(feature["spbT"], iopsBaseline["spbT"], interval)
		updateEvent(&event, "iops", "I/O per second", spaV, spbV, true)
		iopsBaseline = feature

		feature = extractFeature(spMetrics, "sp.*.storage.summary.readBlocks", "sp.*.storage.summary.writeBlocks")
		spaV = getAverage(feature["spaT"], bandwidthBaseline["spaT"], interval)
		spbV = getAverage(feature["spbT"], bandwidthBaseline["spbT"], interval)
		updateEvent(&event, "bandwidth", "Bandwidth(block)", spaV, spbV, true)
		bandwidthBaseline = feature

		bt.client.Publish(event)
		logp.Info("Event sent")
	}
}

// Stop stops unitybeat.
func (bt *Unitybeat) Stop() {
	// Delete Unity queries
	for _, id := range bt.qid {
		go func(qid int) {
			// Just delete the query silently: if the delete fails, the Unity array
			// will delete it automatically after 10 * interval seconds
			bt.box.DeleteMetricRealTimeQuery(qid)
		}(id)
	}
	// Destroy Unity connection silently
	bt.box.Destroy()
	bt.client.Close()
	close(bt.done)
}
