package config

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestCreateSerializerInstantiatorFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tests := []struct {
		name     string
		json     string
		validate func(t *testing.T, s serialization.Serializer)
	}{
		{
			name: "BinarySerializer",
			json: `{"type": "BinarySerializer"}`,
			validate: func(t *testing.T, s serialization.Serializer) {
				assert.IsType(t, &serialization.BinarySerializer{}, s)
			},
		},
		{
			name: "JsonSerializer",
			json: `{"type": "JsonSerializer", "indent": true}`,
			validate: func(t *testing.T, s serialization.Serializer) {
				js, ok := s.(*serialization.JsonSerializer)
				assert.True(t, ok)
				assert.True(t, js.Indent)
			},
		},
		{
			name: "TextSerializer",
			json: `{"type": "TextSerializer"}`,
			validate: func(t *testing.T, s serialization.Serializer) {
				assert.IsType(t, &serialization.TextSerializer{}, s)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diContainer, _ := dependencyinjection.NewContainer()
			inst, err := CreateSerializerInstantiatorFromJson([]byte(tt.json))
			assert.Nil(t, err)
			
			s, err := inst.Instantiate(diContainer)
			assert.Nil(t, err)
			tt.validate(t, s)
		})
	}
}

func TestCreateSerializerInstantiatorFromJson_UnknownType(t *testing.T) {
	testutils.SkipIfIntegration(t)
	_, err := CreateSerializerInstantiatorFromJson([]byte(`{"type": "Unknown"}`))
	assert.NotNil(t, err)
}
