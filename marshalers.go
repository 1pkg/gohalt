package gohalt

import (
	"encoding/json"
)

type Marshaler func(interface{}) ([]byte, error)

var DefaultMarshaler Marshaler = json.Marshal

func marshal(err error) Marshaler {
	return func(interface{}) ([]byte, error) {
		return nil, err
	}
}
