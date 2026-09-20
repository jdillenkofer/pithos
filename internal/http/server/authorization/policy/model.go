package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

const maxPolicyFileSize = 1 << 20

type stringList []string

func (s *stringList) UnmarshalJSON(data []byte) error {
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		*s = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return fmt.Errorf("must be a string or array of strings")
	}
	*s = many
	return nil
}

type File struct {
	SchemaVersion int                 `json:"schemaVersion"`
	Policies      map[string]Document `json:"policies"`
	Bindings      []Binding           `json:"bindings"`
}

type Document struct {
	Version   string          `json:"Version"`
	ID        string          `json:"Id,omitempty"`
	Statement json.RawMessage `json:"Statement"`
}

type Statement struct {
	Sid       string                                `json:"Sid,omitempty"`
	Effect    string                                `json:"Effect"`
	Action    stringList                            `json:"Action"`
	Resource  stringList                            `json:"Resource"`
	Condition map[string]map[string]json.RawMessage `json:"Condition,omitempty"`
}

type Binding struct {
	Policy   string    `json:"policy"`
	Subjects []Subject `json:"subjects"`
}

type Subject struct {
	Type        string `json:"type"`
	AccountID   string `json:"accountId,omitempty"`
	PrincipalID string `json:"principalId,omitempty"`
}

type compiledStatement struct {
	policy, sid, effect string
	actions, resources  []string
	conditions          []condition
}

type condition struct {
	operator string
	key      string
	values   []string
}

type Snapshot struct {
	bySubject map[string][]compiledStatement
}

func strictDecode(data []byte, dst any) error {
	if err := rejectDuplicateKeys(data); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return fmt.Errorf("trailing JSON data")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err != nil {
			return fmt.Errorf("trailing JSON data: %w", err)
		}
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

func rejectDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	_, err := scanJSONValue(dec, "$", nil)
	return err
}

func scanJSONValue(dec *json.Decoder, path string, first json.Token) (json.Token, error) {
	var tok json.Token
	var err error
	if first != nil {
		tok = first
	} else if tok, err = dec.Token(); err != nil {
		return nil, err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	switch d {
	case '{':
		seen := map[string]struct{}{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := kt.(string)
			if !ok {
				return nil, fmt.Errorf("%s: object key must be string", path)
			}
			if _, ok := seen[key]; ok {
				return nil, fmt.Errorf("%s.%s: duplicate JSON key", path, key)
			}
			seen[key] = struct{}{}
			if _, err = scanJSONValue(dec, path+"."+key, nil); err != nil {
				return nil, err
			}
		}
		_, err = dec.Token()
		return tok, err
	case '[':
		i := 0
		for dec.More() {
			if _, err = scanJSONValue(dec, fmt.Sprintf("%s[%d]", path, i), nil); err != nil {
				return nil, err
			}
			i++
		}
		_, err = dec.Token()
		return tok, err
	}
	return tok, nil
}
