package config

import (
	"encoding/json"
	"errors"
	"os"
	"time"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	"golang.org/x/crypto/ssh"
)

type SshClientConfigInstantiator = *SshClientConfigConfiguration

type SshClientConfigConfiguration struct {
	User                        internalConfig.StringProvider   `json:"user"`
	AuthMethodInstantiators     []AuthMethodInstantiator        `json:"-"`
	RawAuthMethods              []json.RawMessage               `json:"authMethods"`
	HostKeyCallbackInstantiator HostKeyCallbackInstantiator     `json:"-"`
	RawHostKeyCallback          json.RawMessage                 `json:"hostKeyCallback"`
	HostKeyAlgorithms           []internalConfig.StringProvider `json:"hostKeyAlgorithms"`
	ConnectionTimeout           Duration                        `json:"connectionTimeout"`
}

func (s *SshClientConfigConfiguration) UnmarshalJSON(b []byte) error {
	type sshClientConfigConfiguration SshClientConfigConfiguration
	err := json.Unmarshal(b, (*sshClientConfigConfiguration)(s))
	if err != nil {
		return err
	}
	for _, rawAuthMethod := range s.RawAuthMethods {
		authMethodInstantiator, err := CreateAuthMethodInstantiatorFromJson(rawAuthMethod)
		if err != nil {
			return err
		}
		s.AuthMethodInstantiators = append(s.AuthMethodInstantiators, authMethodInstantiator)
	}
	s.HostKeyCallbackInstantiator, err = CreateHostKeyCallbackInstantiatorFromJson(s.RawHostKeyCallback)
	if err != nil {
		return err
	}
	return nil
}

func (s *SshClientConfigConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	for _, authMethodInstantiator := range s.AuthMethodInstantiators {
		err := authMethodInstantiator.RegisterReferences(diCollection)
		if err != nil {
			return err
		}
	}
	err := s.HostKeyCallbackInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (s *SshClientConfigConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (*ssh.ClientConfig, error) {
	user := s.User.Value()
	authMethods := []ssh.AuthMethod{}
	for _, authMethodInstantiator := range s.AuthMethodInstantiators {
		authMethod, err := authMethodInstantiator.Instantiate(diProvider)
		if err != nil {
			return nil, err
		}
		authMethods = append(authMethods, authMethod)
	}
	hostKeyCallback, err := s.HostKeyCallbackInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	hostKeyAlgorithms := []string{}
	for _, h := range s.HostKeyAlgorithms {
		hostKeyAlgorithms = append(hostKeyAlgorithms, h.Value())
	}
	connectionTimeout := time.Duration(s.ConnectionTimeout)

	sshClientConfig := ssh.ClientConfig{
		User:              user,
		Auth:              authMethods,
		HostKeyCallback:   hostKeyCallback,
		HostKeyAlgorithms: hostKeyAlgorithms,
		Timeout:           connectionTimeout,
	}
	return &sshClientConfig, nil
}

func CreateSshClientConfigInstantiatorFromJson(b []byte) (SshClientConfigInstantiator, error) {
	var si SshClientConfigInstantiator = &SshClientConfigConfiguration{}
	err := json.Unmarshal(b, &si)
	if err != nil {
		return nil, err
	}
	return si, nil
}

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(tmp)
		return nil
	default:
		return errors.New("invalid duration")
	}
}

const (
	signerWithPassphraseType = "SignerWithPassphrase"
	signerType               = "Signer"
)

type SignerInstantiator = internalConfig.DynamicJsonInstantiator[ssh.Signer]

type SignerWithPassphraseConfiguration struct {
	Path       internalConfig.StringProvider `json:"path"`
	Passphrase internalConfig.StringProvider `json:"passphrase"`
	internalConfig.DynamicJsonType
}

func (s *SignerWithPassphraseConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (s *SignerWithPassphraseConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (ssh.Signer, error) {
	pemBytes, err := os.ReadFile(s.Path.Value())
	if err != nil {
		return nil, err
	}
	privateKey, err := ssh.ParsePrivateKeyWithPassphrase(pemBytes, []byte(s.Passphrase.Value()))
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

type SignerConfiguration struct {
	Path internalConfig.StringProvider `json:"path"`
	internalConfig.DynamicJsonType
}

func (s *SignerConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (s *SignerConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (ssh.Signer, error) {
	pemBytes, err := os.ReadFile(s.Path.Value())
	if err != nil {
		return nil, err
	}
	privateKey, err := ssh.ParsePrivateKey(pemBytes)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

func CreateSignerInstantiatorFromJson(b []byte) (SignerInstantiator, error) {
	var sc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &sc)
	if err != nil {
		return nil, err
	}

	var si SignerInstantiator
	switch sc.Type {
	case signerWithPassphraseType:
		si = &SignerWithPassphraseConfiguration{}
	case signerType:
		si = &SignerConfiguration{}
	default:
		return nil, errors.New("unknown signer type")
	}
	err = json.Unmarshal(b, &si)
	if err != nil {
		return nil, err
	}
	return si, nil
}

const (
	passwordAuthMethodType  = "PasswordAuthMethod"
	publicKeyAuthMethodType = "PublicKeyAuthMethod"
)

type AuthMethodInstantiator = internalConfig.DynamicJsonInstantiator[ssh.AuthMethod]

type PasswordAuthMethodConfiguration struct {
	Password internalConfig.StringProvider `json:"password"`
	internalConfig.DynamicJsonType
}

func (p *PasswordAuthMethodConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (p *PasswordAuthMethodConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (ssh.AuthMethod, error) {
	return ssh.Password(p.Password.Value()), nil
}

type PublicKeyAuthMethodConfiguration struct {
	SignerInstantiators []SignerInstantiator `json:"-"`
	RawSigners          []json.RawMessage    `json:"signers"`
	internalConfig.DynamicJsonType
}

func (p *PublicKeyAuthMethodConfiguration) UnmarshalJSON(b []byte) error {
	type publicKeyAuthMethodConfiguration PublicKeyAuthMethodConfiguration
	err := json.Unmarshal(b, (*publicKeyAuthMethodConfiguration)(p))
	if err != nil {
		return err
	}
	for _, rawSigner := range p.RawSigners {
		signerInstantiator, err := CreateSignerInstantiatorFromJson(rawSigner)
		if err != nil {
			return err
		}
		p.SignerInstantiators = append(p.SignerInstantiators, signerInstantiator)
	}
	return nil
}

func (p *PublicKeyAuthMethodConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	for _, signerInstantiator := range p.SignerInstantiators {
		err := signerInstantiator.RegisterReferences(diCollection)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *PublicKeyAuthMethodConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (ssh.AuthMethod, error) {
	signers := []ssh.Signer{}
	for _, signerInstantiator := range p.SignerInstantiators {
		signer, err := signerInstantiator.Instantiate(diProvider)
		if err != nil {
			return nil, err
		}
		signers = append(signers, signer)
	}
	return ssh.PublicKeys(signers...), nil
}

func CreateAuthMethodInstantiatorFromJson(b []byte) (AuthMethodInstantiator, error) {
	var ac internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &ac)
	if err != nil {
		return nil, err
	}

	var ai AuthMethodInstantiator
	switch ac.Type {
	case passwordAuthMethodType:
		ai = &PasswordAuthMethodConfiguration{}
	case publicKeyAuthMethodType:
		ai = &PublicKeyAuthMethodConfiguration{}
	default:
		return nil, errors.New("unknown authMethod type")
	}
	err = json.Unmarshal(b, &ai)
	if err != nil {
		return nil, err
	}
	return ai, nil
}

const (
	insecureIgnoreHostKeyCallbackType = "InsecureIgnoreHostKeyCallback"
	fixedHostKeyCallbackType          = "FixedHostKeyCallback"
)

type HostKeyCallbackInstantiator = internalConfig.DynamicJsonInstantiator[ssh.HostKeyCallback]

type InsecureIgnoreHostKeyCallbackConfiguration struct {
	internalConfig.DynamicJsonType
}

func (i *InsecureIgnoreHostKeyCallbackConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (i *InsecureIgnoreHostKeyCallbackConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (ssh.HostKeyCallback, error) {
	return ssh.InsecureIgnoreHostKey(), nil
}

type FixedHostKeyCallbackConfiguration struct {
	HostKey string `json:"hostKey"`
	internalConfig.DynamicJsonType
}

func (f *FixedHostKeyCallbackConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (f *FixedHostKeyCallbackConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (ssh.HostKeyCallback, error) {
	hostKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(f.HostKey))
	if err != nil {
		return nil, err
	}
	return ssh.FixedHostKey(hostKey), nil
}

func CreateHostKeyCallbackInstantiatorFromJson(b []byte) (HostKeyCallbackInstantiator, error) {
	var cc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &cc)
	if err != nil {
		return nil, err
	}

	var ci HostKeyCallbackInstantiator
	switch cc.Type {
	case insecureIgnoreHostKeyCallbackType:
		ci = &InsecureIgnoreHostKeyCallbackConfiguration{}
	case fixedHostKeyCallbackType:
		ci = &FixedHostKeyCallbackConfiguration{}
	default:
		return nil, errors.New("unknown hostKeyCallback type")
	}
	err = json.Unmarshal(b, &ci)
	if err != nil {
		return nil, err
	}
	return ci, nil
}
