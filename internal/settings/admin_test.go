package settings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminDefaults(t *testing.T) {
	s := &Settings{}
	assert.True(t, s.AdminEnabled())
	assert.Equal(t, 9001, s.AdminPort())
	assert.Equal(t, s.BindAddress(), s.AdminBindAddress())
	assert.True(t, s.AdminSecureCookie())
	assert.Equal(t, 30, s.AdminSessionIdleMinutes())
	assert.Equal(t, 12, s.AdminSessionMaxHours())
	assert.Equal(t, 0, s.AdminDatabaseIndex())
	assert.Empty(t, s.AdminAccessKeys())
}

func TestAdminRequiresAuthentication(t *testing.T) {
	for _, explicitlyEnabled := range []bool{false, true} {
		s := &Settings{authenticationEnabled: addrOf(false), adminEnabled: addrOf(explicitlyEnabled)}
		assert.False(t, s.AdminEnabled())
	}
	assert.False(t, (&Settings{adminEnabled: addrOf(false)}).AdminEnabled())
}

func TestAdminBindAddressInheritance(t *testing.T) {
	s := &Settings{bindAddress: addrOf("127.0.0.1")}
	assert.Equal(t, "127.0.0.1", s.AdminBindAddress())
	s.adminBindAddress = addrOf("::1")
	assert.Equal(t, "::1", s.AdminBindAddress())
}

func TestAdminCommandLineSettings(t *testing.T) {
	s, err := loadSettingsFromCmdArgs([]string{
		"-adminEnabled=false", "-adminPort=9101", "-adminBindAddress=127.0.0.2",
		"-adminSecureCookie=false", "-adminSessionIdleMinutes=15",
		"-adminSessionMaxHours=8", "-adminDatabaseIndex=2", "-adminAccessKeys= first, ,second ",
	})
	require.NoError(t, err)
	assert.False(t, s.AdminEnabled())
	assert.Equal(t, 9101, s.AdminPort())
	assert.Equal(t, "127.0.0.2", s.AdminBindAddress())
	assert.False(t, s.AdminSecureCookie())
	assert.Equal(t, 15, s.AdminSessionIdleMinutes())
	assert.Equal(t, 8, s.AdminSessionMaxHours())
	assert.Equal(t, 2, s.AdminDatabaseIndex())
	assert.Equal(t, []string{"first", "second"}, s.AdminAccessKeys())
}

func TestAdminEnvironmentSettings(t *testing.T) {
	t.Setenv(adminEnabledEnvKey, "false")
	t.Setenv(adminPortEnvKey, "9101")
	t.Setenv(adminBindAddressEnvKey, "127.0.0.2")
	t.Setenv(adminSecureCookieEnvKey, "false")
	t.Setenv(adminSessionIdleMinutesEnvKey, "15")
	t.Setenv(adminSessionMaxHoursEnvKey, "8")
	t.Setenv(adminDatabaseIndexEnvKey, "2")
	t.Setenv(adminAccessKeysEnvKey, " first, ,second ")
	s, err := loadSettingsFromEnv()
	require.NoError(t, err)
	assert.False(t, s.AdminEnabled())
	assert.Equal(t, 9101, s.AdminPort())
	assert.Equal(t, "127.0.0.2", s.AdminBindAddress())
	assert.False(t, s.AdminSecureCookie())
	assert.Equal(t, 15, s.AdminSessionIdleMinutes())
	assert.Equal(t, 8, s.AdminSessionMaxHours())
	assert.Equal(t, 2, s.AdminDatabaseIndex())
	assert.Equal(t, []string{"first", "second"}, s.AdminAccessKeys())
}

func TestAdminAllowlistMergeAndIsolation(t *testing.T) {
	commandLine, err := loadSettingsFromCmdArgs([]string{"-adminAccessKeys=first"})
	require.NoError(t, err)
	s := mergeSettings(commandLine, &Settings{})
	assert.Equal(t, []string{"first"}, s.AdminAccessKeys())
	keys := s.AdminAccessKeys()
	keys[0] = "changed"
	assert.Equal(t, []string{"first"}, s.AdminAccessKeys())
	s = mergeSettings(s, &Settings{adminAccessKeys: []string{}})
	assert.Empty(t, s.AdminAccessKeys())
}
