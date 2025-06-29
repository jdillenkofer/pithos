package settings

import (
	"flag"
)

func registerStringFlag(flagSet *flag.FlagSet, name string, defaultValue string, description string) func() *string {
	stringVar := flagSet.String(name, defaultValue, description)
	accessor := func() *string {
		found := false
		flagSet.Visit(func(f *flag.Flag) {
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

func registerIntFlag(flagSet *flag.FlagSet, name string, defaultValue int, description string) func() *int {
	intVar := flagSet.Int(name, defaultValue, description)
	accessor := func() *int {
		found := false
		flagSet.Visit(func(f *flag.Flag) {
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

func registerBoolFlag(flagSet *flag.FlagSet, name string, defaultValue bool, description string) func() *bool {
	boolVar := flagSet.Bool(name, defaultValue, description)
	accessor := func() *bool {
		found := false
		flagSet.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		if !found {
			return nil
		}
		return boolVar
	}
	return accessor
}

func loadSettingsFromCmdArgs(cmdArgs []string) (*Settings, error) {
	serveCommand := flag.NewFlagSet("serve", flag.ExitOnError)
	accessKeyIdAccessor := registerStringFlag(serveCommand, "accessKeyId", "", "the access key id")
	secretAccessKeyAccessor := registerStringFlag(serveCommand, "secretAccessKey", "", "the secret access key")
	regionAccessor := registerStringFlag(serveCommand, "region", defaultRegion, "the region for the s3 api")
	domainAccessor := registerStringFlag(serveCommand, "domain", defaultDomain, "the domain for the s3 api")
	bindAddressAccessor := registerStringFlag(serveCommand, "bindAddress", defaultBindAddress, "the address the s3 socket is bound to")
	portAccessor := registerIntFlag(serveCommand, "port", defaultPort, "the port for the s3 api")
	monitoringPortAccessor := registerIntFlag(serveCommand, "monitoringPort", defaultMonitoringPort, "the monitoring port of pithos")
	monitoringPortEnabledAccessor := registerBoolFlag(serveCommand, "monitoringPortEnabled", defaultMonitoringPortEnabled, "determines if the monitoring port of pithos is enabled or not")
	storageJsonPathAccessor := registerStringFlag(serveCommand, "storageJsonPath", defaultStorageJsonPath, "the path to the storage.json configuration")
	authorizerPathAccessor := registerStringFlag(serveCommand, "authorizerPath", defaultAuthorizerPath, "the path to the authorizer script")

	err := serveCommand.Parse(cmdArgs)
	if err != nil {
		return nil, err
	}

	return &Settings{
		accessKeyId:           accessKeyIdAccessor(),
		secretAccessKey:       secretAccessKeyAccessor(),
		region:                regionAccessor(),
		domain:                domainAccessor(),
		bindAddress:           bindAddressAccessor(),
		port:                  portAccessor(),
		monitoringPort:        monitoringPortAccessor(),
		monitoringPortEnabled: monitoringPortEnabledAccessor(),
		storageJsonPath:       storageJsonPathAccessor(),
		authorizerPath:        authorizerPathAccessor(),
	}, nil
}
