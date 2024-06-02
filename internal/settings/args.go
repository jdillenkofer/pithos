package settings

import (
	"flag"
)

func optionalStringFlag(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func loadSettingsFromCmdArgs() (*Settings, error) {
	// TODO: import nullable field handling by checking if a flag was actually set
	// instead of comparing with the default value
	domain := flag.String("domain", "", "the domain for the s3 api")
	bindAddress := flag.String("bindAddress", "", "the address the s3 socket is bound to")
	// TODO: Change port flag to int
	port := flag.String("port", "", "the port for the s3 api")
	storagePath := flag.String("storagePath", "", "the storagePath for metadata and blobs")
	// TODO: How to check if bool flag is not set
	useFilesystemBlobStore := flag.Bool("useFilesystemBlobStore", false, "true if we want to store blobs in the filesystem instead of the sqlite database")
	flag.Parse()
	return &Settings{
		domain:                 optionalStringFlag(*domain),
		bindAddress:            optionalStringFlag(*bindAddress),
		port:                   optionalStringFlag(*port),
		storagePath:            optionalStringFlag(*storagePath),
		useFilesystemBlobStore: useFilesystemBlobStore,
	}, nil
}
