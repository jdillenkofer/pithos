package settings

import (
	"strings"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func addrOf[T any](t T) *T { return &t }

func TestMergeSettingsTwoNils(t *testing.T) {
	testutils.SkipIfIntegration(t)

	a := Settings{
		domain: nil,
	}
	b := Settings{
		domain: nil,
	}
	mergedSettings := mergeSettings(&a, &b)
	assert.NotNil(t, mergedSettings)
	assert.Nil(t, a.domain)
	assert.Nil(t, b.domain)
	assert.Nil(t, mergedSettings.domain)
}

func TestMergeSettingsNilAndValue(t *testing.T) {
	testutils.SkipIfIntegration(t)

	a := Settings{
		domain: nil,
	}
	b := Settings{
		domain: addrOf("test"),
	}
	mergedSettings := mergeSettings(&a, &b)
	assert.NotNil(t, mergedSettings)
	assert.Nil(t, a.domain)
	assert.Equal(t, "test", *b.domain)
	assert.Equal(t, b.domain, mergedSettings.domain)
}

func TestMergeSettingsTwoValues(t *testing.T) {
	testutils.SkipIfIntegration(t)

	a := Settings{
		domain: addrOf("test"),
	}
	b := Settings{
		domain: addrOf("test2"),
	}
	mergedSettings := mergeSettings(&a, &b)
	assert.NotNil(t, mergedSettings)
	assert.Equal(t, "test", *a.domain)
	assert.Equal(t, "test2", *b.domain)
	assert.Equal(t, b.domain, mergedSettings.domain)
}

func TestSpoolDirDefaultsToEmptyOverride(t *testing.T) {
	testutils.SkipIfIntegration(t)

	settings := &Settings{}

	assert.Empty(t, settings.SpoolDir())
}

func TestLoadSpoolDirFromCmdArgs(t *testing.T) {
	testutils.SkipIfIntegration(t)

	settings, err := loadSettingsFromCmdArgs([]string{"-spoolDir", "/var/tmp/pithos"})

	assert.NoError(t, err)
	assert.Equal(t, "/var/tmp/pithos", settings.SpoolDir())
}

func TestLoadSpoolDirFromEnv(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv(spoolDirEnvKey, "/var/tmp/pithos")

	settings, err := loadSettingsFromEnv()

	assert.NoError(t, err)
	assert.Equal(t, "/var/tmp/pithos", settings.SpoolDir())
}

func TestTLSDefaultsRemainHTTPOnly(t *testing.T) {
	testutils.SkipIfIntegration(t)

	appSettings := &Settings{}

	assert.True(t, appSettings.HTTPEnabled())
	assert.False(t, appSettings.HTTPSEnabled())
	assert.Equal(t, 9443, appSettings.HTTPSPort())
	assert.True(t, appSettings.MonitoringPortEnabled())
	assert.False(t, appSettings.MonitoringHTTPSEnabled())
	assert.Equal(t, 9444, appSettings.MonitoringHTTPSPort())
	assert.False(t, appSettings.ACMEEnabled())
	assert.Equal(t, "auto", appSettings.ACMEChallenge())
	assert.NoError(t, appSettings.Validate())
}

func TestTLSSettingsEnvironmentOverridesCommandLine(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv(httpsEnabledEnvKey, "true")
	t.Setenv(httpsPortEnvKey, "10443")
	t.Setenv(tlsCertFileEnvKey, "/env/cert.pem")
	t.Setenv(tlsKeyFileEnvKey, "/env/key.pem")
	t.Setenv(acmeDomainsEnvKey, "example.com, *.example.com")

	appSettings, err := LoadSettings([]string{
		"-httpsEnabled=false",
		"-httpsPort=9443",
		"-tlsCertFile=/cli/cert.pem",
		"-tlsKeyFile=/cli/key.pem",
		"-acmeDomains=cli.example.com",
	})

	assert.NoError(t, err)
	assert.True(t, appSettings.HTTPSEnabled())
	assert.Equal(t, 10443, appSettings.HTTPSPort())
	assert.Equal(t, "/env/cert.pem", appSettings.TLSCertFile())
	assert.Equal(t, "/env/key.pem", appSettings.TLSKeyFile())
	assert.Equal(t, []string{"example.com", "*.example.com"}, appSettings.ACMEDomains())
}

func TestLoadSettingsRejectsInvalidTypedEnvironmentValues(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tests := []struct {
		name   string
		envKey string
		value  string
	}{
		{"authentication enabled", authenticationEnabledEnvKey, "yes"},
		{"HTTP port", portEnvKey, "9000.0"},
		{"HTTP enabled", httpEnabledEnvKey, "enabled"},
		{"HTTPS enabled", httpsEnabledEnvKey, "tru"},
		{"HTTPS port", httpsPortEnvKey, "9443x"},
		{"monitoring port", monitoringPortEnvKey, "9090x"},
		{"monitoring port enabled", monitoringPortEnabledEnvKey, "maybe"},
		{"monitoring HTTPS enabled", monitoringHttpsEnabledEnvKey, "tru"},
		{"monitoring HTTPS port", monitoringHttpsPortEnvKey, "9444x"},
		{"ACME enabled", acmeEnabledEnvKey, "enabled"},
		{"trust forwarded headers", trustForwardedHeadersEnvKey, "yes"},
		{"OpenTelemetry enabled", otelEnabledEnvKey, "enabled"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.envKey, test.value)

			appSettings, err := LoadSettings(nil)

			assert.Nil(t, appSettings)
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), test.envKey)
				assert.Contains(t, err.Error(), test.value)
			}
		})
	}
}

func TestLoadSettingsAcceptsStrictBooleanEnvironmentValues(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv(httpEnabledEnvKey, "FALSE")
	t.Setenv(httpsEnabledEnvKey, "1")

	appSettings, err := LoadSettings(nil)

	assert.NoError(t, err)
	assert.False(t, appSettings.HTTPEnabled())
	assert.True(t, appSettings.HTTPSEnabled())
}

func TestLoadSettingsRejectsInvalidTypedCommandLineValues(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{"HTTPS enabled typo", []string{"-httpsEnabled=tru"}, "invalid boolean value"},
		{"HTTPS port typo", []string{"-httpsPort=9443x"}, "invalid value"},
		{"separate boolean value", []string{"-httpsEnabled", "false"}, "unexpected command-line arguments"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			appSettings, err := LoadSettings(test.args)

			assert.Nil(t, appSettings)
			assert.ErrorContains(t, err, test.want)
		})
	}
}

func TestValidateTLSSettings(t *testing.T) {
	testutils.SkipIfIntegration(t)
	static := func() *Settings {
		return &Settings{
			httpsEnabled: addrOf(true),
			tlsCertFile:  addrOf("cert.pem"),
			tlsKeyFile:   addrOf("key.pem"),
		}
	}
	acme := func() *Settings {
		return &Settings{
			httpsEnabled:        addrOf(true),
			acmeEnabled:         addrOf(true),
			acmeDomains:         []string{"example.com"},
			acmeCacheDir:        addrOf("/data/acme"),
			acmeChallenge:       addrOf("http-01"),
			tlsCertFile:         nil,
			tlsKeyFile:          nil,
			monitoringPort:      addrOf(9090),
			httpsPort:           addrOf(9443),
			monitoringHttpsPort: addrOf(9444),
		}
	}

	tests := []struct {
		name     string
		settings *Settings
		want     string
	}{
		{"no S3 listener", &Settings{httpEnabled: addrOf(false)}, "at least one S3"},
		{"invalid HTTP port", &Settings{port: addrOf(-1)}, "0..65535"},
		{"invalid disabled HTTPS port", &Settings{httpsPort: addrOf(-1)}, "0..65535"},
		{"invalid disabled monitoring port", &Settings{monitoringPortEnabled: addrOf(false), monitoringPort: addrOf(65536)}, "0..65535"},
		{"duplicate address", &Settings{monitoringPort: addrOf(defaultPort)}, "duplicate address"},
		{"HTTPS without source", &Settings{httpsEnabled: addrOf(true)}, "requires static TLS files or ACME"},
		{"certificate without key", &Settings{tlsCertFile: addrOf("cert.pem")}, "both certificate and key"},
		{"source without HTTPS", &Settings{tlsCertFile: addrOf("cert.pem"), tlsKeyFile: addrOf("key.pem")}, "requires an enabled HTTPS"},
		{"static and ACME", func() *Settings {
			s := static()
			s.acmeEnabled = addrOf(true)
			s.acmeDomains = []string{"example.com"}
			s.acmeCacheDir = addrOf("/data/acme")
			return s
		}(), "cannot be enabled together"},
		{"ACME no domains", &Settings{httpsEnabled: addrOf(true), acmeEnabled: addrOf(true)}, "at least one domain"},
		{"ACME no cache", &Settings{httpsEnabled: addrOf(true), acmeEnabled: addrOf(true), acmeDomains: []string{"example.com"}}, "cache directory"},
		{"invalid challenge", func() *Settings {
			s := acme()
			s.acmeChallenge = addrOf("invalid")
			return s
		}(), "unsupported ACME challenge"},
		{"HTTP challenge without HTTP", func() *Settings {
			s := acme()
			s.httpEnabled = addrOf(false)
			s.monitoringPortEnabled = addrOf(false)
			return s
		}(), "requires an enabled HTTP"},
		{"DNS challenge without provider", func() *Settings {
			s := acme()
			s.acmeChallenge = addrOf("dns-01")
			return s
		}(), "requires a DNS provider"},
		{"unsupported DNS provider", func() *Settings {
			s := acme()
			s.acmeChallenge = addrOf("dns-01")
			s.acmeDNSProvider = addrOf("unknown")
			return s
		}(), "unsupported ACME DNS provider"},
		{"DNS provider with HTTP challenge", func() *Settings {
			s := acme()
			s.acmeDNSProvider = addrOf("cloudflare")
			return s
		}(), "only be used with dns-01"},
		{"wildcard without DNS", func() *Settings {
			s := acme()
			s.acmeDomains = []string{"*.example.com"}
			return s
		}(), "requires dns-01"},
		{"valid static", static(), ""},
		{"valid ACME HTTP", acme(), ""},
		{"valid ACME DNS wildcard", func() *Settings {
			s := acme()
			s.acmeChallenge = addrOf("dns-01")
			s.acmeDNSProvider = addrOf("rfc2136")
			s.acmeDomains = []string{"example.com", "*.example.com"}
			return s
		}(), ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.settings.Validate()
			if test.want == "" {
				assert.NoError(t, err)
			} else if assert.Error(t, err) {
				assert.True(t, strings.Contains(err.Error(), test.want), err.Error())
			}
		})
	}
}
