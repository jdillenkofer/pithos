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

func registerBoolFlag(name string, defaultValue bool, description string) func() *bool {
	boolVar := flag.Bool(name, defaultValue, description)
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
	accessKeyIdAccessor := registerStringFlag("accessKeyId", "", "the access key id")
	secretAccessKeyAccessor := registerStringFlag("secretAccessKey", "", "the secret access key")
	regionAccessor := registerStringFlag("region", defaultRegion, "the region for the s3 api")
	domainAccessor := registerStringFlag("domain", defaultDomain, "the domain for the s3 api")
	bindAddressAccessor := registerStringFlag("bindAddress", defaultBindAddress, "the address the s3 socket is bound to")
	portAccessor := registerIntFlag("port", defaultPort, "the port for the s3 api")
	storagePathAccessor := registerStringFlag("storagePath", defaultStoragePath, "the storagePath for metadata and blobs")
	useFilesystemBlobStoreAccessor := registerBoolFlag("useFilesystemBlobStore", defaultUseFilesystemBlobStore, "store blobs in the filesystem instead of the sqlite database")
	wrapBlobStoreWithOutboxAccessor := registerBoolFlag("wrapBlobStoreWithOutbox", defaultWrapBlobStoreWithOutbox, "allows you to use the transactional outbox pattern for storing blobs (default is true, when using the FileSystemBlobStore)")
	flag.Parse()
	return &Settings{
		accessKeyId:             accessKeyIdAccessor(),
		secretAccessKey:         secretAccessKeyAccessor(),
		region:                  regionAccessor(),
		domain:                  domainAccessor(),
		bindAddress:             bindAddressAccessor(),
		port:                    portAccessor(),
		storagePath:             storagePathAccessor(),
		useFilesystemBlobStore:  useFilesystemBlobStoreAccessor(),
		wrapBlobStoreWithOutbox: wrapBlobStoreWithOutboxAccessor(),
	}, nil
}
