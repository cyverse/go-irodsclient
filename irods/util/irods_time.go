package util

import (
	"fmt"
	"strconv"
	"time"
)

// GetIRODSDateTime returns time struct from string IRODS time
func GetIRODSDateTime(timestring string) (time.Time, error) {
	i64, err := strconv.ParseInt(timestring, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("Cannot parse IRODS time string - %s", timestring)
	}
	return time.Unix(i64, 0), nil
}
