package settings

import (
	"encoding/json"
	"os"
)

func loadSettingsFromJson(jsonFile string) (*Settings, error) {
	jsonData, err := os.ReadFile(jsonFile)
	if err != nil {
		return nil, err
	}
	settings := Settings{}
	err = json.Unmarshal(jsonData, &settings)
	if err != nil {
		return nil, err
	}
	return &settings, nil
}
