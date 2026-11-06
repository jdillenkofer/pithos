package admin

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/storage"
)

// simulationInput is one request to evaluate against a policy candidate.
type simulationInput struct {
	Account, Principal, AccessKey string
	Operation, Bucket, Key        string
	SourceBucket, SourceKey       string
}

func simulationInputFrom(form url.Values) simulationInput {
	return simulationInput{
		Account: form.Get("account"), Principal: form.Get("principal"), AccessKey: form.Get("accessKey"),
		Operation: form.Get("operation"), Bucket: form.Get("bucket"), Key: form.Get("key"),
		SourceBucket: form.Get("sourceBucket"), SourceKey: form.Get("sourceKey"),
	}
}

// IsCopy reports whether the operation reads a copy source.
func (in simulationInput) IsCopy() bool {
	return in.Operation == authorization.OperationCopyObject || in.Operation == authorization.OperationUploadPartCopy
}

func (in simulationInput) anonymous() bool { return in.Account == "" }

// simulationResult explains a decision the way S3 would reach it.
type simulationResult struct {
	Allowed            bool
	Title, Explanation string
	Action, Resource   string
	References         []authorization.StatementReference
	// PolicyEffect is the policy's own decision when the account boundary
	// rejects the request before policies would run.
	PolicyEffect string
	Context      []keyValue
}

// simulatorIdentity prefills the simulator from a known credential.
type simulatorIdentity struct {
	AccessKey, Account, Principal string
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func validateSimulationInput(in simulationInput) error {
	if !slices.Contains(policy.SupportedOperations(), in.Operation) {
		return errors.New("choose an operation")
	}
	if (in.Account == "") != (in.Principal == "") {
		return errors.New("enter both account and principal, or neither for an anonymous request")
	}
	// Every authenticated S3 request is signed with an access key; without one
	// the authorizers treat the request as anonymous.
	if (in.Account == "") != (in.AccessKey == "") {
		return errors.New("authenticated requests need an account, a principal and an access key; leave all three empty for an anonymous request")
	}
	if len(in.Account) > authentication.MaxAccountIDLength || len(in.Principal) > 256 || len(in.AccessKey) > authentication.MaxAccessKeyIDLength {
		return errors.New("identity fields are too long")
	}
	check := func(label, bucket, key string, keyRequired bool) error {
		if bucket == "" {
			if key != "" || keyRequired {
				return fmt.Errorf("enter a %s bucket", label)
			}
			return nil
		}
		if _, err := storage.NewBucketName(bucket); err != nil {
			return fmt.Errorf("invalid %s bucket name", label)
		}
		if key == "" {
			if keyRequired {
				return fmt.Errorf("enter a %s key", label)
			}
			return nil
		}
		if _, err := storage.NewObjectKey(key); err != nil {
			return fmt.Errorf("invalid %s key", label)
		}
		return nil
	}
	if err := check("target", in.Bucket, in.Key, false); err != nil {
		return err
	}
	if in.IsCopy() {
		return check("copy source", in.SourceBucket, in.SourceKey, true)
	}
	return nil
}

// simulationRequest builds the request the S3 server would authorize: the
// caller identity, the authentication context and the resolved bucket owner.
func simulationRequest(in simulationInput, owner, sourceOwner *string) *authorization.Request {
	request := &authorization.Request{
		Operation: in.Operation, Bucket: optional(in.Bucket), Key: optional(in.Key), ResourceAccountId: owner,
		Authorization: authorization.Authorization{AuthType: "Anonymous"},
	}
	if !in.anonymous() {
		request.Authorization = authorization.Authorization{
			AccountId: optional(in.Account), PrincipalId: optional(in.Principal), AccessKeyId: optional(in.AccessKey),
			AuthType: "REST-HEADER", SignatureVersion: "AWS4-HMAC-SHA256",
		}
	}
	if in.IsCopy() {
		request.SourceBucket, request.SourceKey, request.SourceResourceAccountId = optional(in.SourceBucket), optional(in.SourceKey), sourceOwner
		request.CopyTaggingDirective = "COPY"
	}
	return request
}

// bucketOwnerFunc resolves a bucket's owner; found is false for a missing bucket.
type bucketOwnerFunc func(ctx context.Context, bucket string) (owner string, found bool, err error)

func (a *App) bucketOwner(ctx context.Context, bucket string) (string, bool, error) {
	if a.options.Storage == nil {
		return "", false, errors.New("bucket owners cannot be looked up without storage")
	}
	name, err := storage.NewBucketName(bucket)
	if err != nil {
		return "", false, err
	}
	head, err := a.options.Storage.HeadBucket(ctx, name)
	if errors.Is(err, storage.ErrNoSuchBucket) {
		return "", false, nil
	}
	if err != nil {
		return "", false, errors.New("unable to look up the bucket owner")
	}
	return head.OwnerAccountID, true, nil
}

func effectExplanation(kind string, effect authorization.Effect) (bool, string, string) {
	switch effect {
	case authorization.Allow:
		if kind == "lua" {
			return true, "Allowed", "The Lua policy returned true."
		}
		return true, "Allowed", "An Allow statement matches and no Deny statement does."
	case authorization.ExplicitDeny:
		if kind == "lua" {
			return false, "Denied", "The Lua policy returned false."
		}
		return false, "Denied explicitly", "A Deny statement matches. Explicit denies override every Allow."
	default:
		return false, "Denied implicitly", "No statement allows this request, so it is denied by default."
	}
}

// simulate evaluates in against the candidate evaluator and applies the S3
// server's account boundary: authenticated callers may only access buckets
// their account owns, before any policy runs.
func simulate(ctx context.Context, kind string, evaluator authorization.RequestAuthorizer, in simulationInput, ownerOf bucketOwnerFunc) (simulationResult, error) {
	if err := validateSimulationInput(in); err != nil {
		return simulationResult{}, err
	}
	var result simulationResult
	if in.anonymous() {
		result.Context = append(result.Context, keyValue{"Caller", "Anonymous"})
	} else {
		caller := "Principal " + in.Principal + " in account " + in.Account + " (access key " + in.AccessKey + ")"
		result.Context = append(result.Context, keyValue{"Caller", caller}, keyValue{"Authentication", "Signature V4 header"})
	}

	boundary := ""
	resolve := func(label, bucket string) (*string, error) {
		if bucket == "" {
			return nil, nil
		}
		owner, found, err := ownerOf(ctx, bucket)
		if err != nil {
			return nil, err
		}
		switch {
		case !found && in.Operation == authorization.OperationCreateBucket && label == "Bucket owner":
			result.Context = append(result.Context, keyValue{label, "New bucket, owned by the caller"})
			return optional(in.Account), nil
		case !found:
			return nil, fmt.Errorf("bucket %q does not exist; S3 would answer NoSuchBucket", bucket)
		}
		result.Context = append(result.Context, keyValue{label, "Account " + owner})
		if !in.anonymous() && owner != in.Account && boundary == "" {
			boundary = fmt.Sprintf("Bucket %s is owned by account %s. S3 rejects requests from other accounts before evaluating policies.", bucket, owner)
		}
		return &owner, nil
	}
	owner, err := resolve("Bucket owner", in.Bucket)
	if err != nil {
		return simulationResult{}, err
	}
	var sourceOwner *string
	if in.IsCopy() {
		if sourceOwner, err = resolve("Copy source owner", in.SourceBucket); err != nil {
			return simulationResult{}, err
		}
	}

	decision, err := evaluator.AuthorizeRequest(ctx, simulationRequest(in, owner, sourceOwner))
	if err != nil {
		return simulationResult{}, err
	}
	result.Action, result.Resource, result.References = decision.Action, decision.Resource, decision.References
	result.Allowed, result.Title, result.Explanation = effectExplanation(kind, decision.Effect)
	if boundary != "" {
		_, policyTitle, _ := effectExplanation(kind, decision.Effect)
		result.Allowed, result.Title, result.Explanation, result.PolicyEffect = false, "Denied by the account boundary", boundary, policyTitle
	}
	return result, nil
}
