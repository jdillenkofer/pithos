package integrity

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type ValidationResult struct {
	BucketName     string
	ObjectKey      string
	Success        bool
	ErrorType      string
	PartFailures   []PartFailure
	ObjectFailures []string
	ActionTaken    string // e.g., "None", "Deleted"
}

type PartFailure struct {
	PartID         string
	SequenceNumber int
	Error          string
}

type ValidationReport struct {
	StartTime         time.Time
	EndTime           time.Time
	TotalBuckets      int
	TotalObjects      int
	SuccessfulObjects int
	FailedObjects     int
	DeletedObjects    int
	Results           []ValidationResult
}

func (r *ValidationReport) HasFailures() bool {
	return r.FailedObjects > 0
}

func OutputJSON(report *ValidationReport, w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func OutputHumanReadable(report *ValidationReport, w io.Writer) error {
	fmt.Fprintf(w, "Storage Integrity Validation Report\n")
	fmt.Fprintf(w, "===================================\n")
	fmt.Fprintf(w, "Start Time: %s\n", report.StartTime.Format(time.RFC3339))
	fmt.Fprintf(w, "End Time:   %s\n", report.EndTime.Format(time.RFC3339))
	fmt.Fprintf(w, "Duration:   %s\n\n", report.EndTime.Sub(report.StartTime))

	fmt.Fprintf(w, "Summary:\n")
	fmt.Fprintf(w, "  Total Buckets:      %d\n", report.TotalBuckets)
	fmt.Fprintf(w, "  Total Objects:      %d\n", report.TotalObjects)
	fmt.Fprintf(w, "  Successful Objects: %d\n", report.SuccessfulObjects)
	fmt.Fprintf(w, "  Failed Objects:     %d\n", report.FailedObjects)
	fmt.Fprintf(w, "  Deleted Objects:    %d\n\n", report.DeletedObjects)

	if len(report.Results) > 0 {
		fmt.Fprintf(w, "Failures:\n")
		for _, result := range report.Results {
			if !result.Success {
				fmt.Fprintf(w, "  Bucket: %s, Key: %s\n", result.BucketName, result.ObjectKey)
				fmt.Fprintf(w, "    Error: %s\n", result.ErrorType)
				if len(result.PartFailures) > 0 {
					fmt.Fprintf(w, "    Part Failures:\n")
					for _, bf := range result.PartFailures {
						fmt.Fprintf(w, "      - PartID: %s, Seq: %d, Error: %s\n", bf.PartID, bf.SequenceNumber, bf.Error)
					}
				}
				if len(result.ObjectFailures) > 0 {
					fmt.Fprintf(w, "    Object Failures:\n")
					for _, of := range result.ObjectFailures {
						fmt.Fprintf(w, "      - %s\n", of)
					}
				}
				if result.ActionTaken != "" {
					fmt.Fprintf(w, "    Action Taken: %s\n", result.ActionTaken)
				}
				fmt.Fprintf(w, "\n")
			}
		}
	}

	return nil
}
