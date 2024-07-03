package types

import "time"

// Message struct for kafka communication
type Message struct {
	AppID   int
	TS      time.Time
	Message string
}
