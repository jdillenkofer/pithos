package settings

import (
	"flag"
	"strings"
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
	authenticationEnabledAccessor := registerBoolFlag(serveCommand, "authenticationEnabled", defaultAuthenticationEnabled, "determines if authentication is enabled or not")
	regionAccessor := registerStringFlag(serveCommand, "region", defaultRegion, "the region for the s3 api")
	domainAccessor := registerStringFlag(serveCommand, "domain", defaultDomain, "the domain for the s3 api")
	websiteDomainAccessor := registerStringFlag(serveCommand, "websiteDomain", defaultWebsiteDomain, "the domain for s3 website hosting (e.g. s3-website.localhost)")
	bindAddressAccessor := registerStringFlag(serveCommand, "bindAddress", defaultBindAddress, "the address the s3 socket is bound to")
	portAccessor := registerIntFlag(serveCommand, "port", defaultPort, "the port for the s3 api")
	monitoringPortAccessor := registerIntFlag(serveCommand, "monitoringPort", defaultMonitoringPort, "the monitoring port of pithos")
	monitoringPortEnabledAccessor := registerBoolFlag(serveCommand, "monitoringPortEnabled", defaultMonitoringPortEnabled, "determines if the monitoring port of pithos is enabled or not")
	storageJsonPathAccessor := registerStringFlag(serveCommand, "storageJsonPath", defaultStorageJsonPath, "the path to the storage.json configuration")
	authorizerPathAccessor := registerStringFlag(serveCommand, "authorizerPath", defaultAuthorizerPath, "the path to the authorizer script")
	trustForwardedHeadersAccessor := registerBoolFlag(serveCommand, "trustForwardedHeaders", defaultTrustForwardedHeaders, "trust client forwarding headers (X-Forwarded-For, X-Forwarded-Proto, CF-Connecting-IP)")
	trustedProxyCIDRsAccessor := registerStringFlag(serveCommand, "trustedProxyCIDRs", "", "comma-separated trusted proxy CIDR ranges; empty means all proxies when trustForwardedHeaders is enabled")
	logLevelAccessor := registerStringFlag(serveCommand, "logLevel", "info", "the log level for the application (debug, info, warn, error, fatal)")
	otelEnabledAccessor := registerBoolFlag(serveCommand, "otelEnabled", defaultOtelEnabled, "determines if opentelemetry is enabled or not")
	otelExporterAccessor := registerStringFlag(serveCommand, "otelExporter", defaultOtelExporter, "the exporter for opentelemetry (stdout, otlp)")
	otelEndpointAccessor := registerStringFlag(serveCommand, "otelEndpoint", defaultOtelEndpoint, "the endpoint for the opentelemetry exporter")

	err := serveCommand.Parse(cmdArgs)
	if err != nil {
		return nil, err
	}

	var trustedProxyCIDRs []string
	trustedProxyCIDRsRaw := trustedProxyCIDRsAccessor()
	if trustedProxyCIDRsRaw != nil {
		parts := strings.Split(*trustedProxyCIDRsRaw, ",")
		trustedProxyCIDRs = make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				trustedProxyCIDRs = append(trustedProxyCIDRs, trimmed)
			}
		}
	}

	return &Settings{
		authenticationEnabled: authenticationEnabledAccessor(),
		credentials:           nil, // Credentials are not set via command line args
		region:                regionAccessor(),
		domain:                domainAccessor(),
		websiteDomain:         websiteDomainAccessor(),
		bindAddress:           bindAddressAccessor(),
		port:                  portAccessor(),
		monitoringPort:        monitoringPortAccessor(),
		monitoringPortEnabled: monitoringPortEnabledAccessor(),
		storageJsonPath:       storageJsonPathAccessor(),
		authorizerPath:        authorizerPathAccessor(),
		trustForwardedHeaders: trustForwardedHeadersAccessor(),
		trustedProxyCIDRs:     trustedProxyCIDRs,
		logLevel:              logLevelAccessor(),
		otelEnabled:           otelEnabledAccessor(),
		otelExporter:          otelExporterAccessor(),
		otelEndpoint:          otelEndpointAccessor(),
	}, nil
}
