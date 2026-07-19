package config

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"golang.org/x/term"
)

type DynamicJsonType struct {
	Type string `json:"type"`
}

type DynamicJsonInstantiator[T any] interface {
	RegisterReferences(diCollection dependencyinjection.DICollection) error
	Instantiate(diProvider dependencyinjection.DIProvider) (T, error)
}

type DbContainer struct {
	dbs []database.Database
}

func NewDbContainer() *DbContainer {
	return &DbContainer{}
}

func (dbContainer *DbContainer) AddDb(db database.Database) {
	dbContainer.dbs = append(dbContainer.dbs, db)
}

func (dbContainer *DbContainer) Dbs() []database.Database {
	return dbContainer.dbs
}

func CreateTempDir() (tempDir *string, cleanup func(), err error) {
	d, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		return nil, nil, err
	}
	tempDir = &d
	cleanup = func() {
		_ = os.RemoveAll(*tempDir)
	}
	return
}

const (
	envKeyType = "EnvKey"
	stdinType  = "Stdin"
	fileType   = "File"
)

type envKeyProvider struct {
	EnvKey string `json:"envKey"`
	DynamicJsonType
}

type stdinProvider struct {
	Prompt string `json:"prompt"`
	Hidden bool   `json:"hidden"`
	DynamicJsonType
}

type fileProvider struct {
	Path string `json:"path"`
	DynamicJsonType
}

type StringProvider struct {
	value  string `json:"-"`
	envKey string `json:"-"`
	file   string `json:"-"`
}

func (s *StringProvider) Value() string {
	return s.value
}

func (s *StringProvider) SetValue(value string) {
	s.value = value
}

func (s *StringProvider) WriteValue(value string) error {
	switch {
	case s.envKey != "":
		if err := os.Setenv(s.envKey, value); err != nil {
			return err
		}
	case s.file != "":
		if err := os.WriteFile(s.file, []byte(value), 0o600); err != nil {
			return err
		}
	default:
		return errors.New("string provider does not support writing back")
	}
	s.value = value
	return nil
}

func (s *StringProvider) UnmarshalJSON(b []byte) error {
	var rawString string
	err := json.Unmarshal(b, &rawString)
	if err == nil {
		s.value = rawString
		return nil
	}

	// Try to unmarshal as a generic type to determine which provider to use
	var dt DynamicJsonType
	if err = json.Unmarshal(b, &dt); err != nil {
		return err
	}

	switch dt.Type {
	case envKeyType:
		ekp := envKeyProvider{}
		if err = json.Unmarshal(b, &ekp); err != nil {
			return err
		}
		s.envKey = ekp.EnvKey
		s.value = os.Getenv(ekp.EnvKey)
		return nil
	case stdinType:
		sp := stdinProvider{}
		if err = json.Unmarshal(b, &sp); err != nil {
			return err
		}
		s.value, err = readFromStdin(sp.Prompt, sp.Hidden)
		return err
	case fileType:
		fp := fileProvider{}
		if err = json.Unmarshal(b, &fp); err != nil {
			return err
		}
		data, err := os.ReadFile(fp.Path)
		if err != nil {
			return err
		}
		s.file = fp.Path
		s.value = string(data)
		return nil
	default:
		return errors.New("invalid stringProvider type")
	}
}

func readFromStdin(prompt string, hidden bool) (string, error) {
	if prompt != "" {
		fmt.Print(prompt)
	}

	if hidden {
		// Read password without echoing to terminal
		byteValue, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", err
		}
		// Print newline since ReadPassword doesn't echo the Enter key
		fmt.Println()
		return string(byteValue), nil
	}

	// Read regular input
	reader := bufio.NewReader(os.Stdin)
	value, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(value, "\n"), nil
}

type Int64Provider struct {
	value int64
}

func (i *Int64Provider) Value() int64 {
	return i.value
}

func (i *Int64Provider) UnmarshalJSON(b []byte) error {
	var rawInt64 int64
	err := json.Unmarshal(b, &rawInt64)
	if err == nil {
		i.value = rawInt64
		return nil
	}
	ekp := envKeyProvider{}
	err = json.Unmarshal(b, &ekp)
	if err != nil {
		return err
	}
	if ekp.Type != envKeyType {
		return errors.New("invalid int64Provider type")
	}
	envInt64 := os.Getenv(ekp.EnvKey)
	i.value, err = strconv.ParseInt(envInt64, 10, 64)
	if err != nil {
		return err
	}
	return nil
}

type BoolProvider struct {
	value bool
}

func (i *BoolProvider) Value() bool {
	return i.value
}

func (i *BoolProvider) UnmarshalJSON(b []byte) error {
	var rawBool bool
	err := json.Unmarshal(b, &rawBool)
	if err == nil {
		i.value = rawBool
		return nil
	}
	ekp := envKeyProvider{}
	err = json.Unmarshal(b, &ekp)
	if err != nil {
		return err
	}
	if ekp.Type != envKeyType {
		return errors.New("invalid boolProvider type")
	}
	envBool := os.Getenv(ekp.EnvKey)
	i.value, err = strconv.ParseBool(envBool)
	if err != nil {
		return err
	}
	return nil
}

type Float64Provider struct {
	value float64
}

func (f *Float64Provider) Value() float64 {
	return f.value
}

func (f *Float64Provider) UnmarshalJSON(b []byte) error {
	var rawFloat64 float64
	err := json.Unmarshal(b, &rawFloat64)
	if err == nil {
		f.value = rawFloat64
		return nil
	}
	ekp := envKeyProvider{}
	err = json.Unmarshal(b, &ekp)
	if err != nil {
		return err
	}
	if ekp.Type != envKeyType {
		return errors.New("invalid float64Provider type")
	}
	envFloat64 := os.Getenv(ekp.EnvKey)
	f.value, err = strconv.ParseFloat(envFloat64, 64)
	if err != nil {
		return err
	}
	return nil
}
