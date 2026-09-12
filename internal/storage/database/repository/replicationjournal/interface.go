package replicationjournal

import (
	"context"
	"database/sql"
	"io"
)

type Operation struct {
	ID            string
	ReplicationID string
	Bucket        string
	Key           string
	Name          string
	Payload       string
	PrimaryResult *string
	State         string
	Attempts      int
	LastError     string
}

type Mapping struct {
	ReplicationID     string
	SecondaryID       string
	Bucket            string
	Key               string
	Kind              string
	PrimaryID         string
	SecondaryObjectID string
}

type Repository interface {
	RegisterTopology(context.Context, *sql.Tx, string, []string) error
	SaveOperation(context.Context, *sql.Tx, *Operation) error
	Pending(context.Context, *sql.Tx, string) ([]Operation, error)
	Acknowledge(context.Context, *sql.Tx, string, string, string) error
	Acknowledgments(context.Context, *sql.Tx, string) (map[string]string, error)
	SaveProgress(context.Context, *sql.Tx, string, string, string) error
	FindProgress(context.Context, *sql.Tx, string, string) (*string, error)
	DeleteProgress(context.Context, *sql.Tx, string, string) error
	SaveMapping(context.Context, *sql.Tx, Mapping) error
	FindMapping(context.Context, *sql.Tx, Mapping) (*string, error)
	SaveData(context.Context, *sql.Tx, string, io.Reader) error
	ReadData(context.Context, *sql.Tx, string) (io.ReadCloser, error)
	DeleteData(context.Context, *sql.Tx, string) error
}
