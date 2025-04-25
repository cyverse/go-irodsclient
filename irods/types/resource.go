package types

import (
	"fmt"
	"time"
)

// IRODSResource describes a resource host
type IRODSResource struct {
	RescID   int64  `json:"resc_id"`
	Name     string `json:"name"`
	Zone     string `json:"zone"`
	Type     string `json:"type"`
	Class    string `json:"class"`
	Location string `json:"location"`

	// Path has the path string of the resource
	Path string `json:"path"`

	// Context has the context string
	Context string `json:"context"`

	// CreateTime has creation time
	CreateTime time.Time `json:"create_time"`
	// ModifyTime has last modified time
	ModifyTime time.Time `json:"modify_time"`
}

// ToString stringifies the object
func (res *IRODSResource) ToString() string {
	return fmt.Sprintf("<IRODSResource %s: %v>", res.Name, res)
}
