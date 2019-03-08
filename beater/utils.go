package beater

import (
	"errors"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/kckecheng/storagemetric/dell/emc/unity"
	"math"
	"strconv"
)

func newUnity(bt *Unitybeat) (*unity.Unity, error) {
	host := bt.config.Host
	username := bt.config.Username
	password := bt.config.Password

	if host == "" || username == "" || password == "" {
		return nil, errors.New("Unity address(IP/FQDN), username and password must be specified.")
	}

	box, err := unity.New(host, username, password)
	if err != nil {
		return nil, err
	} else {
		return box, nil
	}
}

func updateEvent(event *beat.Event, field string, description string, spaV float64, spbV float64, calSum bool) {
	if calSum {
		event.Fields[field] = map[string]interface{}{
			"description": description,
			"spa":         spaV,
			"spb":         spbV,
			"total":       spaV + spbV,
		}
	} else {
		event.Fields[field] = map[string]interface{}{
			"description": description,
			"spa":         spaV,
			"spb":         spbV,
		}
	}
}

func getFloat64(v interface{}) float64 {
	var ret float64
	switch v.(type) {
	case float64:
		ret = v.(float64)
	case string:
		ret, _ = strconv.ParseFloat(v.(string), 64)
	}
	return ret
}

func getAverage(v, b, i float64) float64 {
	return math.Floor((v - b) / i)
}
