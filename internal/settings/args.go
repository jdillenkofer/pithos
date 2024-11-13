package settings

import (
	"flag"
)

func registerStringFlag(name string, defaultValue string, description string) func() *string {
	stringVar := flag.String(name, defaultValue, description)
	accessor := func() *string {
		found := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		if !found {
			return nil
		}
		return stringVar
	}
	return accessor
}

func registerIntFlag(name string, defaultValue int, description string) func() *int {
	intVar := flag.Int(name, defaultValue, description)
	accessor := func() *int {
		found := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		if !found {
			return nil
		}
		return intVar
	}
	return accessor
}

func loadSettingsFromCmdArgs() (*Settings, error) {
	accessKeyIdAccessor := registerStringFlag("accessKeyId", "", "the access key id")
	secretAccessKeyAccessor := registerStringFlag("secretAccessKey", "", "the secret access key")
	regionAccessor := registerStringFlag("region", defaultRegion, "the region for the s3 api")
	domainAccessor := registerStringFlag("domain", defaultDomain, "the domain for the s3 api")
	bindAddressAccessor := registerStringFlag("bindAddress", defaultBindAddress, "the address the s3 socket is bound to")
	portAccessor := registerIntFlag("port", defaultPort, "the port for the s3 api")
	monitoringPortAccessor := registerIntFlag("monitoringPort", defaultMonitoringPort, "the monitoring port of pithos")
	storageJsonPathAccessor := registerStringFlag("storageJsonPath", defaultStorageJsonPath, "the path to the storage.json configuration")

	flag.Parse()

	return &Settings{
		accessKeyId:     accessKeyIdAccessor(),
		secretAccessKey: secretAccessKeyAccessor(),
		region:          regionAccessor(),
		domain:          domainAccessor(),
		bindAddress:     bindAddressAccessor(),
		port:            portAccessor(),
		monitoringPort:  monitoringPortAccessor(),
		storageJsonPath: storageJsonPathAccessor(),
	}, nil
}
