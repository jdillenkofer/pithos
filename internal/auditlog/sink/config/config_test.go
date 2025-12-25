package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestFileSinkConfiguration_Instantiate(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir, err := os.MkdirTemp("", "sink-config-test")
	assert.Nil(t, err)
	defer os.RemoveAll(tempDir)

	path := filepath.Join(tempDir, "audit.log")
	jsonData := fmt.Sprintf(`{
		"type": "FileSink",
		"path": %s,
		"serializer": {
			"type": "BinarySerializer"
		}
	}`, strconv.Quote(path))

	diContainer, _ := dependencyinjection.NewContainer()
	inst, err := CreateSinkInstantiatorFromJson([]byte(jsonData))
	assert.Nil(t, err)

	s, err := inst.Instantiate(diContainer)
	assert.Nil(t, err)
	assert.NotNil(t, s)
	assert.IsType(t, &sink.WriterSink{}, s)
	
	// Verify it implements InitialStateProvider
	isp, ok := s.(sink.InitialStateProvider)
	assert.True(t, ok)
	_, err = isp.InitialState()
	assert.Nil(t, err)

	err = s.Close()
	assert.Nil(t, err)
}

func TestCreateSinkInstantiatorFromJson_UnknownType(t *testing.T) {
	testutils.SkipIfIntegration(t)
	_, err := CreateSinkInstantiatorFromJson([]byte(`{"type": "Unknown"}`))
	assert.NotNil(t, err)
}
