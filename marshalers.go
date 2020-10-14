package gohalt

import "encoding/json"

// Marshaler defined by typical marshaler func signature.
type Marshaler func(interface{}) ([]byte, error)

// DefaultMarshaler defines default marshaler value used by `WithMarshaler`.
// By default DefaultMarshaler is set to use `json.Marshal`.
var DefaultMarshaler Marshaler = json.Marshal

func marshal(err error) Marshaler {
	return func(interface{}) ([]byte, error) {
		return nil, err
	}
}
