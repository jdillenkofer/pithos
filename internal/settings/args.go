package settings

import (
	"flag"
)

func registerStringFlag(name string, description string) func() *string {
	stringVar := flag.String(name, "", description)
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

func registerIntFlag(name string, description string) func() *int {
	intVar := flag.Int(name, 0, description)
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

func registerBoolFlag(name string, description string) func() *bool {
	boolVar := flag.Bool(name, false, description)
	accessor := func() *bool {
		found := false
		flag.Visit(func(f *flag.Flag) {
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

func loadSettingsFromCmdArgs() (*Settings, error) {
	domainAccessor := registerStringFlag("domain", "the domain for the s3 api")
	bindAddressAccessor := registerStringFlag("bindAddress", "the address the s3 socket is bound to")
	portAccessor := registerIntFlag("port", "the port for the s3 api")
	storagePathAccessor := registerStringFlag("storagePath", "the storagePath for metadata and blobs")
	useFilesystemBlobStoreAccessor := registerBoolFlag("useFilesystemBlobStore", "true if we want to store blobs in the filesystem instead of the sqlite database")
	flag.Parse()
	return &Settings{
		domain:                 domainAccessor(),
		bindAddress:            bindAddressAccessor(),
		port:                   portAccessor(),
		storagePath:            storagePathAccessor(),
		useFilesystemBlobStore: useFilesystemBlobStoreAccessor(),
	}, nil
}
