package settings

import (
	"flag"
	"fmt"
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
	serveCommand := flag.NewFlagSet("serve", flag.ContinueOnError)
	authenticationEnabledAccessor := registerBoolFlag(serveCommand, "authenticationEnabled", defaultAuthenticationEnabled, "determines if authentication is enabled or not")
	regionAccessor := registerStringFlag(serveCommand, "region", defaultRegion, "the region for the s3 api")
	domainAccessor := registerStringFlag(serveCommand, "domain", defaultDomain, "the domain for the s3 api")
	websiteDomainAccessor := registerStringFlag(serveCommand, "websiteDomain", defaultWebsiteDomain, "the domain for s3 website hosting (e.g. s3-website.localhost)")
	bindAddressAccessor := registerStringFlag(serveCommand, "bindAddress", defaultBindAddress, "the address the s3 socket is bound to")
	portAccessor := registerIntFlag(serveCommand, "port", defaultPort, "the port for the s3 api")
	httpEnabledAccessor := registerBoolFlag(serveCommand, "httpEnabled", defaultHTTPEnabled, "determines if the HTTP listener for the s3 api is enabled")
	httpsEnabledAccessor := registerBoolFlag(serveCommand, "httpsEnabled", defaultHTTPSEnabled, "determines if the HTTPS listener for the s3 api is enabled")
	httpsPortAccessor := registerIntFlag(serveCommand, "httpsPort", defaultHTTPSPort, "the HTTPS port for the s3 api")
	monitoringPortAccessor := registerIntFlag(serveCommand, "monitoringPort", defaultMonitoringPort, "the monitoring port of pithos")
	monitoringPortEnabledAccessor := registerBoolFlag(serveCommand, "monitoringPortEnabled", defaultMonitoringPortEnabled, "determines if the monitoring port of pithos is enabled or not")
	monitoringHttpsEnabledAccessor := registerBoolFlag(serveCommand, "monitoringHttpsEnabled", defaultMonitoringHTTPSEnabled, "determines if the HTTPS monitoring port is enabled")
	monitoringHttpsPortAccessor := registerIntFlag(serveCommand, "monitoringHttpsPort", defaultMonitoringHTTPSPort, "the HTTPS monitoring port of pithos")
	tlsCertFileAccessor := registerStringFlag(serveCommand, "tlsCertFile", "", "the PEM certificate chain used by HTTPS listeners")
	tlsKeyFileAccessor := registerStringFlag(serveCommand, "tlsKeyFile", "", "the PEM private key used by HTTPS listeners")
	acmeEnabledAccessor := registerBoolFlag(serveCommand, "acmeEnabled", defaultACMEEnabled, "enable automatic ACME certificate management")
	acmeDomainsAccessor := registerStringFlag(serveCommand, "acmeDomains", "", "comma-separated domains included in the managed certificate")
	acmeEmailAccessor := registerStringFlag(serveCommand, "acmeEmail", "", "optional ACME account email")
	acmeCacheDirAccessor := registerStringFlag(serveCommand, "acmeCacheDir", "", "persistent directory for ACME certificates and account data")
	acmeCADirectoryURLAccessor := registerStringFlag(serveCommand, "acmeCADirectoryURL", defaultACMECADirectoryURL, "ACME CA directory URL")
	acmeChallengeAccessor := registerStringFlag(serveCommand, "acmeChallenge", defaultACMEChallenge, "ACME challenge type (auto, http-01, tls-alpn-01, dns-01)")
	acmeDNSProviderAccessor := registerStringFlag(serveCommand, "acmeDNSProvider", "", "lego DNS provider used for dns-01")
	storageJsonPathAccessor := registerStringFlag(serveCommand, "storageJsonPath", defaultStorageJsonPath, "the path to the storage.json configuration")
	authorizerPathAccessor := registerStringFlag(serveCommand, "authorizerPath", defaultAuthorizerPath, "the path to the authorizer script")
	spoolDirAccessor := registerStringFlag(serveCommand, "spoolDir", defaultSpoolDir, "the directory for temporary spool files")
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
	if serveCommand.NArg() != 0 {
		return nil, fmt.Errorf("unexpected command-line arguments: %q", serveCommand.Args())
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

	var acmeDomains []string
	acmeDomainsRaw := acmeDomainsAccessor()
	if acmeDomainsRaw != nil {
		acmeDomains = splitCommaSeparated(*acmeDomainsRaw)
	}

	return &Settings{
		authenticationEnabled:  authenticationEnabledAccessor(),
		credentials:            nil, // Credentials are not set via command line args
		region:                 regionAccessor(),
		domain:                 domainAccessor(),
		websiteDomain:          websiteDomainAccessor(),
		bindAddress:            bindAddressAccessor(),
		port:                   portAccessor(),
		httpEnabled:            httpEnabledAccessor(),
		httpsEnabled:           httpsEnabledAccessor(),
		httpsPort:              httpsPortAccessor(),
		monitoringPort:         monitoringPortAccessor(),
		monitoringPortEnabled:  monitoringPortEnabledAccessor(),
		monitoringHttpsEnabled: monitoringHttpsEnabledAccessor(),
		monitoringHttpsPort:    monitoringHttpsPortAccessor(),
		tlsCertFile:            tlsCertFileAccessor(),
		tlsKeyFile:             tlsKeyFileAccessor(),
		acmeEnabled:            acmeEnabledAccessor(),
		acmeDomains:            acmeDomains,
		acmeEmail:              acmeEmailAccessor(),
		acmeCacheDir:           acmeCacheDirAccessor(),
		acmeCADirectoryURL:     acmeCADirectoryURLAccessor(),
		acmeChallenge:          acmeChallengeAccessor(),
		acmeDNSProvider:        acmeDNSProviderAccessor(),
		storageJsonPath:        storageJsonPathAccessor(),
		authorizerPath:         authorizerPathAccessor(),
		spoolDir:               spoolDirAccessor(),
		trustForwardedHeaders:  trustForwardedHeadersAccessor(),
		trustedProxyCIDRs:      trustedProxyCIDRs,
		logLevel:               logLevelAccessor(),
		otelEnabled:            otelEnabledAccessor(),
		otelExporter:           otelExporterAccessor(),
		otelEndpoint:           otelEndpointAccessor(),
	}, nil
}
