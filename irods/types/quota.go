package types

import (
	"fmt"
)

// IRODSQuota describes a resource quota
type IRODSQuota struct {
	RescName string
	Limit    int64
}

// ToString stringifies the object
func (q *IRODSQuota) ToString() string {
	return fmt.Sprintf("<IRODSQuota %s: %v>", q.RescName, q.Limit)
}
