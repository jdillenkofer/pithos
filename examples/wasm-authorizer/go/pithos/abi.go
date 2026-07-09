package pithos

import "encoding/json"

type Input struct {
	Hook     string   `json:"hook"`
	Request  Request  `json:"request"`
	Resource Resource `json:"resource"`
}

type Request struct {
	Operation     string        `json:"operation"`
	Authorization Authorization `json:"authorization"`
	Bucket        string        `json:"bucket"`
	Key           string        `json:"key"`
	IsReadOnly    bool          `json:"isReadOnly"`
}

type Authorization struct {
	AccessKeyID string `json:"accessKeyId"`
}

type Resource struct {
	Key string `json:"key"`
}

type Decision struct {
	Allow bool `json:"allow"`
}

func ParseInput(inputBytes []byte) (Input, bool) {
	var input Input
	if err := json.Unmarshal(inputBytes, &input); err != nil {
		return Input{}, false
	}
	return input, true
}

func MarshalDecision(allow bool) []byte {
	output, err := json.Marshal(Decision{Allow: allow})
	if err != nil {
		return []byte(`{"allow":false}`)
	}
	return output
}
