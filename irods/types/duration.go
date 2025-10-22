package types

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/errors"
)

// Duration is a replacement of time.Duration that supports JSON
type Duration time.Duration

// MarshalJSON ...
func (d *Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(*d).String())
}

// UnmarshalJSON ...
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return errors.Wrapf(err, "failed to parse %q to time.Duration", string(b))
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return errors.Wrapf(err, "failed to parse %q to time.Duration", string(b))
		}
		*d = Duration(tmp)
		return nil
	default:
		return errors.Errorf("failed to parse %q to time.Duration", string(b))
	}
}

// bug in YAMLv2, fixed in YAMLv3
// // MarshalYAML ...
// func (d *Duration) MarshalYAML() (interface{}, error) {
// 	return time.Duration(*d).String(), nil
// }

// UnmarshalYAML ...
func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var tm string
	if err := unmarshal(&tm); err != nil {
		return errors.Wrapf(err, "failed to parse %q to time.Duration", tm)
	}

	lastChar := byte(tm[len(tm)-1])
	if lastChar >= '0' && lastChar <= '9' {
		// ends with number, no units
		tm = tm + "ns"
	}

	td, err := time.ParseDuration(tm)
	if err != nil {
		return errors.Wrapf(err, "failed to parse %q to time.Duration", tm)
	}

	*d = Duration(td)
	return nil
}
