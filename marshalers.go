package gohalt

import "encoding/json"

type Marshaler func(interface{}) ([]byte, error)

var DefaultMarshaler Marshaler = json.Marshal
