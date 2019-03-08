package beater

import (
	"github.com/kckecheng/storagemetric/dell/emc/unity"
	"sync"
)

var MetricPathSet = [][]string{
	[]string{"sp.*.cpu.summary.busyTicks", "sp.*.cpu.summary.idleTicks"},
	[]string{"sp.*.storage.summary.reads", "sp.*.storage.summary.writes", "sp.*.storage.summary.readBlocks", "sp.*.storage.summary.writeBlocks"},
}

type spMetric struct {
	path string
	spa  float64
	spb  float64
}

func newQuery(box *unity.Unity, interval int) ([]int, error) {
	ids := []int{}
	size := len(MetricPathSet)
	idsC := make(chan int, size)
	errC := make(chan error, size)

	var wg sync.WaitGroup
	for _, paths := range MetricPathSet {
		wg.Add(1)
		go func(paths []string) {
			id, err := box.NewMetricRealTimeQuery(paths, interval)
			if err != nil {
				errC <- err
				close(errC)
			} else {
				idsC <- id
			}
			wg.Done()
		}(paths)
	}
	wg.Wait()
	close(errC)

	err, ok := <-errC
	if ok {
		return ids, err
	} else {
		for i := 0; i < size; i++ {
			id := <-idsC
			ids = append(ids, id)
		}
		close(idsC)
		return ids, nil
	}
}

func getMetrics(box *unity.Unity, ids []int) []unity.Metric {
	metrics := []unity.Metric{}
	metricC := make(chan unity.Metric, len(ids))
	errC := make(chan error, len(ids))

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id int) {
			var ret unity.Metric
			err := box.GetMetricQueryResult(id, &ret)
			if err != nil {
				errC <- err
				close(errC)
			} else {
				metricC <- ret
			}
			wg.Done()
		}(id)
	}
	wg.Wait()
	close(errC)

	_, ok := <-errC
	if ok {
		return metrics
	} else {
		for i := 0; i < len(ids); i++ {
			metric := <-metricC
			metrics = append(metrics, metric)
		}
		close(metricC)
		return metrics
	}
}

func extractSPMetrics(metrics []unity.Metric) []spMetric {
	var spMetrics []spMetric
	for _, metric := range metrics {
		for _, entry := range metric.Entries {
			content := entry.Content
			v := spMetric{content.Path, getFloat64(content.Values.Spa), getFloat64(content.Values.Spb)}
			spMetrics = append(spMetrics, v)
		}
	}
	return spMetrics
}

func extracSPMetricValue(spMetrics []spMetric, path string) (float64, float64) {
	var spa, spb float64
	for _, metric := range spMetrics {
		if metric.path == path {
			spa, spb = metric.spa, metric.spb
			break
		}
	}
	return spa, spb
}

func extractFeature(spMetrics []spMetric, rpath string, wpath string) map[string]float64 {
	spa_v1, spb_v1 := extracSPMetricValue(spMetrics, rpath)
	spa_v2, spb_v2 := extracSPMetricValue(spMetrics, wpath)
	var feature = map[string]float64{
		"spaR": spa_v1,
		"spaW": spa_v2,
		"spbR": spb_v1,
		"spbW": spb_v2,
		"spaT": spa_v1 + spa_v2,
		"spbT": spb_v1 + spb_v2,
		"rT":   spa_v1 + spb_v1,
		"wT":   spa_v2 + spb_v2,
	}
	return feature
}
