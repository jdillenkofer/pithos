// Package config provides JSON configuration for tape devices. Instantiators
// return a DeviceOpener closure instead of an open device, so that slow
// device operations (cartridge load) happen at lifecycle start rather than
// at configuration parse time.
package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"github.com/jdillenkofer/pithos/internal/tape"
	"github.com/jdillenkofer/pithos/internal/tape/simulator"
	"github.com/jdillenkofer/pithos/internal/tape/st"
)

const (
	simulatorTapeDeviceType = "SimulatorTapeDevice"
	stTapeDeviceType        = "StTapeDevice"

	latencyProfileNone = "none"
)

// DeviceOpener opens a tape device; it is called at lifecycle start.
type DeviceOpener = func(ctx context.Context) (tape.Device, error)

type TapeDeviceInstantiator = internalConfig.DynamicJsonInstantiator[DeviceOpener]

type SimulatorTapeDeviceConfiguration struct {
	Path          internalConfig.StringProvider `json:"path"`
	CapacityBytes internalConfig.Int64Provider  `json:"capacityBytes,omitempty"`
	// Latency selects the simulated drive timing: "" or "none" disables
	// latency; "lto1" through "lto10" simulate the corresponding LTO class.
	Latency  internalConfig.StringProvider `json:"latency,omitempty"`
	ReadOnly internalConfig.BoolProvider   `json:"readOnly,omitempty"`
	internalConfig.DynamicJsonType
}

func (s *SimulatorTapeDeviceConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (s *SimulatorTapeDeviceConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (DeviceOpener, error) {
	var latency simulator.LatencyProfile
	switch s.Latency.Value() {
	case "", latencyProfileNone:
	default:
		value := s.Latency.Value()
		generation, err := strconv.Atoi(strings.TrimPrefix(value, "lto"))
		if err != nil || value != fmt.Sprintf("lto%d", generation) {
			return nil, fmt.Errorf("unknown tape latency profile %q", s.Latency.Value())
		}
		var ok bool
		latency, ok = simulator.LTOProfile(generation)
		if !ok {
			return nil, fmt.Errorf("unknown tape latency profile %q", s.Latency.Value())
		}
	}
	path := s.Path.Value()
	opts := simulator.Options{
		Capacity: s.CapacityBytes.Value(),
		Latency:  latency,
		ReadOnly: s.ReadOnly.Value(),
	}
	return func(ctx context.Context) (tape.Device, error) {
		return simulator.Open(ctx, path, opts)
	}, nil
}

type StTapeDeviceConfiguration struct {
	// Path is the tape device node, e.g. /dev/nst0 (use the non-rewinding
	// device).
	Path internalConfig.StringProvider `json:"path"`
	internalConfig.DynamicJsonType
}

func (s *StTapeDeviceConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (s *StTapeDeviceConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (DeviceOpener, error) {
	path := s.Path.Value()
	return func(ctx context.Context) (tape.Device, error) {
		return st.Open(path)
	}, nil
}

func CreateTapeDeviceInstantiatorFromJson(b []byte) (TapeDeviceInstantiator, error) {
	var bc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &bc)
	if err != nil {
		return nil, err
	}

	var bi TapeDeviceInstantiator
	switch bc.Type {
	case simulatorTapeDeviceType:
		bi = &SimulatorTapeDeviceConfiguration{}
	case stTapeDeviceType:
		bi = &StTapeDeviceConfiguration{}
	default:
		return nil, errors.New("unknown tapeDevice type")
	}
	err = json.Unmarshal(b, &bi)
	if err != nil {
		return nil, err
	}
	return bi, nil
}
