package goes

import "github.com/satori/go.uuid"

// NewUUID returns a new V4 uuid as a string
func NewUUID() string {
	return uuid.NewV4().String()
}
