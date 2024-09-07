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
	metricPortAccessor := registerIntFlag("metricPort", defaultMetricPort, "the metric port of pithos")
	storagePathAccessor := registerStringFlag("storagePath", defaultStoragePath, "the storagePath for metadata and blobs")
	useFilesystemBlobStoreAccessor := registerBoolFlag("useFilesystemBlobStore", defaultUseFilesystemBlobStore, "store blobs in the filesystem instead of the sqlite database")
	useEncryptedBlobStoreAccessor := registerBoolFlag("useEncryptedBlobStore", defaultUseEncryptedBlobStore, "encrypt blobs before storing them")
	wrapBlobStoreWithOutboxAccessor := registerBoolFlag("wrapBlobStoreWithOutbox", defaultWrapBlobStoreWithOutbox, "allows you to use the transactional outbox pattern for storing blobs (default is true, when using the FileSystemBlobStore)")

	replicationAccessKeyIdAccessor := registerStringFlag("replicationAccessKeyId", "", "the replication access key id")
	replicationSecretAccessKeyAccessor := registerStringFlag("replicationSecretAccessKey", "", "the replication secret access key")
	replicationRegionAccessor := registerStringFlag("replicationRegion", defaultRegion, "the replication region for the s3 api")
	replicationEndpointAccessor := registerStringFlag("replicationEndpoint", "", "the replication endpoint for the s3 api")
	replicationUseOutboxAccessor := registerBoolFlag("replicationUseOutbox", defaultReplicationUseOutbox, "use transactional outbox pattern for replication")

	flag.Parse()

	replicationAccessKeyId := replicationAccessKeyIdAccessor()
	replicationSecretAccessKey := replicationSecretAccessKeyAccessor()
	replicationRegion := replicationRegionAccessor()
	replicationEndpoint := replicationEndpointAccessor()
	replicationUseOutbox := replicationUseOutboxAccessor()

	var replication *ReplicationSettings = nil
	if replicationAccessKeyId != nil && replicationSecretAccessKey != nil && replicationRegion != nil {
		replication = &ReplicationSettings{
			accessKeyId:     replicationAccessKeyId,
			secretAccessKey: replicationSecretAccessKey,
			region:          replicationRegion,
			endpoint:        replicationEndpoint,
			useOutbox:       replicationUseOutbox,
		}
	}

	return &Settings{
		accessKeyId:             accessKeyIdAccessor(),
		secretAccessKey:         secretAccessKeyAccessor(),
		region:                  regionAccessor(),
		domain:                  domainAccessor(),
		bindAddress:             bindAddressAccessor(),
		port:                    portAccessor(),
		metricPort:              metricPortAccessor(),
		storagePath:             storagePathAccessor(),
		useFilesystemBlobStore:  useFilesystemBlobStoreAccessor(),
		useEncryptedBlobStore:   useEncryptedBlobStoreAccessor(),
		wrapBlobStoreWithOutbox: wrapBlobStoreWithOutboxAccessor(),
		replication:             replication,
	}, nil
}
