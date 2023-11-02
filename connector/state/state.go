package state

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// Codec is used to encode and decode state objects into plain strings
type Codec interface {

	//Encode encodes a struct into a simple string
	Encode(d any) (string, error)

	//Decode decodes a string into a given struct
	Decode(state string, d any) error
}

type jsonBase64Codec struct {
}

// NewJsonBase64Codec create a new codec using base64 encoded json to encode state
func NewJsonBase64Codec() Codec {
	return &jsonBase64Codec{}
}

func (j *jsonBase64Codec) Encode(d any) (string, error) {
	bytes, err := json.Marshal(d)
	if err != nil {
		return "", err
	}

	s := base64.URLEncoding.EncodeToString(bytes)
	return s, nil
}

func (j *jsonBase64Codec) Decode(state string, d any) error {
	state = strings.TrimSpace(state)
	if state == "" {
		return nil
	}

	bytes, err := base64.URLEncoding.DecodeString(state)
	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, d)
}
