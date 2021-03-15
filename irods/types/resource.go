package types

import (
	"fmt"
	"time"
)

// IRODSResource describes a resource host
type IRODSResource struct {
	RescID   int64
	Name     string
	Zone     string
	Type     string
	Class    string
	Location string

	// Path has the path string of the resource
	Path string

	// Context has the context string
	Context string

	// CreateTime has creation time
	CreateTime time.Time
	// ModifyTime has last modified time
	ModifyTime time.Time
}

// ToString stringifies the object
func (res *IRODSResource) ToString() string {
	return fmt.Sprintf("<IRODSResource %s: %v>", res.Name, res)
}
