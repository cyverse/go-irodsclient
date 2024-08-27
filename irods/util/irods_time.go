package util

import (
	"strconv"
	"time"

	"golang.org/x/xerrors"
)

// GetIRODSDateTime returns time struct from string IRODS time
func GetIRODSDateTime(timestring string) (time.Time, error) {
	i64, err := strconv.ParseInt(timestring, 10, 64)
	if err != nil {
		return time.Time{}, xerrors.Errorf("cannot parse IRODS time string %q", timestring)
	}

	if i64 <= 0 {
		return time.Time{}, nil
	}

	return time.Unix(i64, 0), nil
}

// GetIRODSDateTimeStringForTicket returns IRODS time string from time struct
func GetIRODSDateTimeStringForTicket(t time.Time) string {
	if t.IsZero() {
		return "0"
	}

	return t.UTC().Format("2006-01-02.15:04:05")
}
