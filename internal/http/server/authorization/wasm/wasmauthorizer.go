package wasm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/textproto"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	exportAlloc      = "pithos_alloc"
	exportFree       = "pithos_free"
	exportEvaluate   = "pithos_evaluate"
	exportInitialize = "_initialize"
	exportMemory     = "memory"

	hookRequest             = "request"
	hookListBucket          = "list-bucket"
	hookListObject          = "list-object"
	hookDeleteObjectEntry   = "delete-object-entry"
	hookListMultipartUpload = "list-multipart-upload"
	hookListPart            = "list-part"

	defaultTimeout          = 100 * time.Millisecond
	defaultMemoryLimitPages = 64
	defaultInstancePoolSize = 0
)

var (
	errAllocMissing    = errors.New("wasm authorizer export pithos_alloc not found")
	errFreeMissing     = errors.New("wasm authorizer export pithos_free not found")
	errEvaluateMissing = errors.New("wasm authorizer export pithos_evaluate not found")
	errMemoryMissing   = errors.New("wasm authorizer export memory not found")
)

type WasmAuthorizer struct {
	runtime               wazero.Runtime
	compiled              wazero.CompiledModule
	instancePool          chan api.Module
	instanceCounter       atomic.Uint64
	timeout               time.Duration
	trustForwardedHeaders bool
	trustedProxyCIDRs     []*net.IPNet
	tracer                trace.Tracer
}

type Options struct {
	Timeout               time.Duration
	MemoryLimitPages      uint32
	InstancePoolSize      int
	TrustForwardedHeaders bool
	TrustedProxyCIDRs     []string
}

type input struct {
	Hook     string    `json:"hook"`
	Request  request   `json:"request"`
	Resource *resource `json:"resource,omitempty"`
}

type resource struct {
	BucketName *string `json:"bucketName,omitempty"`
	Key        *string `json:"key,omitempty"`
	UploadID   *string `json:"uploadId,omitempty"`
	PartNumber *int32  `json:"partNumber,omitempty"`
}

type request struct {
	Operation         string            `json:"operation"`
	Authorization     auth              `json:"authorization"`
	Bucket            *string           `json:"bucket,omitempty"`
	Key               *string           `json:"key,omitempty"`
	SourceBucket      *string           `json:"sourceBucket,omitempty"`
	SourceKey         *string           `json:"sourceKey,omitempty"`
	HTTPRequest       httpRequest       `json:"httpRequest"`
	IsReadOnly        bool              `json:"isReadOnly"`
	RequestObjectTags map[string]string `json:"requestObjectTags,omitempty"`
	ObjectTags        map[string]string `json:"objectTags,omitempty"`
	SourceObjectTags  map[string]string `json:"sourceObjectTags,omitempty"`
}

type auth struct {
	AccessKeyID *string `json:"accessKeyId,omitempty"`
}

type httpRequest struct {
	Method        string       `json:"method"`
	Path          string       `json:"path"`
	Query         string       `json:"query"`
	QueryParams   []nameValues `json:"queryParams,omitempty"`
	Headers       []nameValues `json:"headers,omitempty"`
	Host          string       `json:"host"`
	Proto         string       `json:"proto"`
	ContentLength *int         `json:"contentLength,omitempty"`
	RemoteAddr    string       `json:"remoteAddr"`
	RemoteIP      *string      `json:"remoteIP,omitempty"`
	ClientIP      *string      `json:"clientIP,omitempty"`
	Scheme        string       `json:"scheme"`
}

type nameValues struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

type decision struct {
	Allow  bool    `json:"allow"`
	Reason *string `json:"reason,omitempty"`
}

func NewWasmAuthorizer(wasmBytes []byte) (*WasmAuthorizer, error) {
	return NewWasmAuthorizerWithOptions(wasmBytes, Options{})
}

func NewWasmAuthorizerWithOptions(wasmBytes []byte, options Options) (*WasmAuthorizer, error) {
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	memoryLimitPages := options.MemoryLimitPages
	if memoryLimitPages == 0 {
		memoryLimitPages = defaultMemoryLimitPages
	}
	instancePoolSize := options.InstancePoolSize
	if instancePoolSize == defaultInstancePoolSize {
		instancePoolSize = runtime.GOMAXPROCS(0)
	}
	if instancePoolSize < 0 {
		instancePoolSize = 0
	}

	ctx := context.Background()
	runtimeConfig := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(memoryLimitPages)
	runtime := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		_ = runtime.Close(ctx)
		return nil, err
	}
	compiled, err := runtime.CompileModule(ctx, wasmBytes)
	if err != nil {
		_ = runtime.Close(ctx)
		return nil, err
	}

	authorizer := &WasmAuthorizer{
		runtime:               runtime,
		compiled:              compiled,
		instancePool:          make(chan api.Module, instancePoolSize),
		timeout:               timeout,
		trustForwardedHeaders: options.TrustForwardedHeaders,
		trustedProxyCIDRs:     parseTrustedProxyCIDRs(options.TrustedProxyCIDRs),
		tracer:                otel.Tracer("internal/http/server/authorization/wasm"),
	}
	if err := authorizer.dryRun(); err != nil {
		_ = runtime.Close(ctx)
		return nil, err
	}
	return authorizer, nil
}

func (authorizer *WasmAuthorizer) Close(ctx context.Context) error {
	for {
		select {
		case mod := <-authorizer.instancePool:
			_ = mod.Close(ctx)
		default:
			return authorizer.runtime.Close(ctx)
		}
	}
}

func (authorizer *WasmAuthorizer) getModule(ctx context.Context) (api.Module, error) {
	select {
	case mod := <-authorizer.instancePool:
		return mod, nil
	default:
	}

	instanceID := authorizer.instanceCounter.Add(1)
	mod, err := authorizer.runtime.InstantiateModule(ctx, authorizer.compiled, wazero.NewModuleConfig().WithName(fmt.Sprintf("pithos-authorizer-%d", instanceID)))
	if err != nil {
		return nil, err
	}
	if initialize := mod.ExportedFunction(exportInitialize); initialize != nil {
		if _, err := initialize.Call(ctx); err != nil {
			_ = mod.Close(context.Background())
			return nil, err
		}
	}
	return mod, nil
}

func (authorizer *WasmAuthorizer) putModule(mod api.Module, discard bool) {
	if discard {
		_ = mod.Close(context.Background())
		return
	}
	select {
	case authorizer.instancePool <- mod:
	default:
		_ = mod.Close(context.Background())
	}
}

func (authorizer *WasmAuthorizer) dryRun() error {
	_, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationPutObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptr("AKIAIOSFODNN7EXAMPLE"),
		},
	})
	return err
}

func (authorizer *WasmAuthorizer) AuthorizeRequest(ctx context.Context, request *authorization.Request) (bool, error) {
	return authorizer.evaluate(ctx, hookRequest, request, nil)
}

func (authorizer *WasmAuthorizer) AuthorizeListBucket(ctx context.Context, request *authorization.Request, bucketName string) (bool, error) {
	return authorizer.evaluate(ctx, hookListBucket, request, &resource{BucketName: &bucketName})
}

func (authorizer *WasmAuthorizer) AuthorizeListObject(ctx context.Context, request *authorization.Request, key string) (bool, error) {
	return authorizer.evaluate(ctx, hookListObject, request, &resource{Key: &key})
}

func (authorizer *WasmAuthorizer) AuthorizeDeleteObjectEntry(ctx context.Context, request *authorization.Request, key string) (bool, error) {
	return authorizer.evaluate(ctx, hookDeleteObjectEntry, request, &resource{Key: &key})
}

func (authorizer *WasmAuthorizer) AuthorizeListMultipartUpload(ctx context.Context, request *authorization.Request, key string, uploadID string) (bool, error) {
	return authorizer.evaluate(ctx, hookListMultipartUpload, request, &resource{Key: &key, UploadID: &uploadID})
}

func (authorizer *WasmAuthorizer) AuthorizeListPart(ctx context.Context, request *authorization.Request, partNumber int32) (bool, error) {
	return authorizer.evaluate(ctx, hookListPart, request, &resource{PartNumber: &partNumber})
}

func (authorizer *WasmAuthorizer) evaluate(ctx context.Context, hook string, authorizationRequest *authorization.Request, res *resource) (bool, error) {
	ctx, span := authorizer.tracer.Start(ctx, "WasmAuthorizer.AuthorizeRequest")
	defer span.End()

	callCtx, cancel := context.WithTimeout(ctx, authorizer.timeout)
	defer cancel()

	inputBytes, err := authorizer.marshalInput(callCtx, hook, authorizationRequest, res)
	if err != nil {
		return false, err
	}

	mod, err := authorizer.getModule(callCtx)
	if err != nil {
		return false, err
	}
	discardModule := false
	defer func() {
		authorizer.putModule(mod, discardModule)
	}()

	alloc := mod.ExportedFunction(exportAlloc)
	if alloc == nil {
		return false, errAllocMissing
	}
	free := mod.ExportedFunction(exportFree)
	if free == nil {
		return false, errFreeMissing
	}
	evaluate := mod.ExportedFunction(exportEvaluate)
	if evaluate == nil {
		return false, errEvaluateMissing
	}
	memory := mod.ExportedMemory(exportMemory)
	if memory == nil {
		return false, errMemoryMissing
	}

	inputPtr, err := callAlloc(callCtx, alloc, len(inputBytes))
	if err != nil {
		return false, err
	}
	defer callFree(context.Background(), free, inputPtr, uint32(len(inputBytes)))

	if !memory.Write(inputPtr, inputBytes) {
		return false, fmt.Errorf("failed to write wasm authorization input at ptr=%d len=%d", inputPtr, len(inputBytes))
	}

	results, err := evaluate.Call(callCtx, uint64(inputPtr), uint64(len(inputBytes)))
	if err != nil {
		discardModule = true
		return false, err
	}
	if len(results) != 1 {
		return false, fmt.Errorf("wasm authorizer returned %d results, expected 1", len(results))
	}

	outputPtr, outputLen := unpackPtrLen(results[0])
	defer callFree(context.Background(), free, outputPtr, outputLen)

	outputBytes, ok := memory.Read(outputPtr, outputLen)
	if !ok {
		return false, fmt.Errorf("failed to read wasm authorization output at ptr=%d len=%d", outputPtr, outputLen)
	}
	var dec decision
	if err := json.Unmarshal(outputBytes, &dec); err != nil {
		return false, err
	}
	slog.DebugContext(ctx, "Authorization result", "operation", authorizationRequest.Operation, "isAuthorized", dec.Allow, "reason", dec.Reason)
	return dec.Allow, nil
}

func (authorizer *WasmAuthorizer) marshalInput(ctx context.Context, hook string, authorizationRequest *authorization.Request, res *resource) ([]byte, error) {
	objectTags, err := resolveTags(ctx, authorizationRequest.ResolveExistingObjectTags)
	if err != nil {
		return nil, err
	}
	sourceObjectTags, err := resolveTags(ctx, authorizationRequest.ResolveExistingSourceObjectTags)
	if err != nil {
		return nil, err
	}

	clientIP, scheme := authorizer.resolveClientIPAndScheme(authorizationRequest.HttpRequest)
	req := input{
		Hook: hook,
		Request: request{
			Operation: authorizationRequest.Operation,
			Authorization: auth{
				AccessKeyID: authorizationRequest.Authorization.AccessKeyId,
			},
			Bucket:            authorizationRequest.Bucket,
			Key:               authorizationRequest.Key,
			SourceBucket:      authorizationRequest.SourceBucket,
			SourceKey:         authorizationRequest.SourceKey,
			HTTPRequest:       projectHTTPRequest(authorizationRequest.HttpRequest, clientIP, scheme),
			IsReadOnly:        isReadOnly(authorizationRequest.Operation),
			RequestObjectTags: copyStringMap(authorizationRequest.RequestObjectTags),
			ObjectTags:        objectTags,
			SourceObjectTags:  sourceObjectTags,
		},
		Resource: res,
	}
	return json.Marshal(req)
}

func projectHTTPRequest(req authorization.HTTPRequest, clientIP *string, scheme string) httpRequest {
	return httpRequest{
		Method:        req.Method,
		Path:          req.Path,
		Query:         req.Query,
		QueryParams:   mapToNameValues(req.QueryParams, false),
		Headers:       mapToNameValues(req.Headers, true),
		Host:          req.Host,
		Proto:         req.Proto,
		ContentLength: req.ContentLength,
		RemoteAddr:    req.RemoteAddr,
		RemoteIP:      req.RemoteIP,
		ClientIP:      clientIP,
		Scheme:        scheme,
	}
}

func callAlloc(ctx context.Context, alloc wazeroFunction, size int) (uint32, error) {
	if size < 0 {
		return 0, fmt.Errorf("invalid wasm allocation size %d", size)
	}
	results, err := alloc.Call(ctx, uint64(uint32(size)))
	if err != nil {
		return 0, err
	}
	if len(results) != 1 {
		return 0, fmt.Errorf("wasm allocator returned %d results, expected 1", len(results))
	}
	return uint32(results[0]), nil
}

type wazeroFunction interface {
	Call(ctx context.Context, params ...uint64) ([]uint64, error)
}

func callFree(ctx context.Context, free wazeroFunction, ptr uint32, size uint32) {
	_, _ = free.Call(ctx, uint64(ptr), uint64(size))
}

func unpackPtrLen(v uint64) (uint32, uint32) {
	return uint32(v >> 32), uint32(v)
}

func resolveTags(ctx context.Context, resolver func(context.Context) (map[string]string, error)) (map[string]string, error) {
	if resolver == nil {
		return nil, nil
	}
	tags, err := resolver(ctx)
	if err != nil {
		return nil, err
	}
	return copyStringMap(tags), nil
}

func copyStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func mapToNameValues(values map[string][]string, canonicalize bool) []nameValues {
	if len(values) == 0 {
		return nil
	}
	result := make([]nameValues, 0, len(values))
	for name, vals := range values {
		if canonicalize {
			name = textproto.CanonicalMIMEHeaderKey(name)
		}
		result = append(result, nameValues{Name: name, Values: append([]string(nil), vals...)})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

func ptr[T any](value T) *T {
	return &value
}

func isReadOnly(operation string) bool {
	switch operation {
	case authorization.OperationListBuckets, authorization.OperationHeadBucket, authorization.OperationHeadObject, authorization.OperationHeadObjectVersion, authorization.OperationListMultipartUploads, authorization.OperationListObjects, authorization.OperationListParts, authorization.OperationGetObject, authorization.OperationGetObjectVersion, authorization.OperationGetBucketWebsite, authorization.OperationGetBucketCORS, authorization.OperationGetBucketNotification, authorization.OperationGetObjectTagging, authorization.OperationGetObjectVersionTagging:
		return true
	default:
		return false
	}
}

func parseTrustedProxyCIDRs(cidrStrings []string) []*net.IPNet {
	if len(cidrStrings) == 0 {
		return nil
	}
	parsed := make([]*net.IPNet, 0, len(cidrStrings))
	for _, cidrStr := range cidrStrings {
		_, ipNet, err := net.ParseCIDR(cidrStr)
		if err != nil {
			slog.Warn("Ignoring invalid trusted proxy CIDR", "cidr", cidrStr, "error", err)
			continue
		}
		parsed = append(parsed, ipNet)
	}
	return parsed
}

func (authorizer *WasmAuthorizer) resolveClientIPAndScheme(httpRequest authorization.HTTPRequest) (*string, string) {
	clientIP := httpRequest.RemoteIP
	scheme := httpRequest.Scheme
	if scheme == "" {
		scheme = "http"
	}

	if !authorizer.trustForwardedHeaders || !isTrustedProxy(httpRequest.RemoteIP, authorizer.trustedProxyCIDRs) {
		return clientIP, scheme
	}

	if cfConnectingIP := getHeaderIgnoreCase(httpRequest.Headers, "CF-Connecting-IP"); cfConnectingIP != nil {
		if ip := net.ParseIP(strings.TrimSpace(*cfConnectingIP)); ip != nil {
			parsedIP := ip.String()
			clientIP = &parsedIP
		}
	} else if xForwardedFor := getHeaderIgnoreCase(httpRequest.Headers, "X-Forwarded-For"); xForwardedFor != nil {
		if parsed := parseForwardedClientIP(*xForwardedFor); parsed != nil {
			clientIP = parsed
		}
	}

	if xForwardedProto := getHeaderIgnoreCase(httpRequest.Headers, "X-Forwarded-Proto"); xForwardedProto != nil {
		if parsedScheme := parseForwardedScheme(*xForwardedProto); parsedScheme != nil {
			scheme = *parsedScheme
		}
	}

	return clientIP, scheme
}

func isTrustedProxy(remoteIP *string, trustedProxyCIDRs []*net.IPNet) bool {
	if remoteIP == nil {
		return false
	}
	ip := net.ParseIP(*remoteIP)
	if ip == nil {
		return false
	}
	if len(trustedProxyCIDRs) == 0 {
		return true
	}
	for _, cidr := range trustedProxyCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

func getHeaderValuesCaseInsensitive(headers map[string][]string, key string) []string {
	canonicalKey := textproto.CanonicalMIMEHeaderKey(key)
	if values, ok := headers[canonicalKey]; ok {
		return values
	}
	for headerName, values := range headers {
		if strings.EqualFold(headerName, key) {
			return values
		}
	}
	return nil
}

func getHeaderIgnoreCase(headers map[string][]string, key string) *string {
	values := getHeaderValuesCaseInsensitive(headers, key)
	if len(values) == 0 {
		return nil
	}
	value := values[0]
	return &value
}

func parseForwardedClientIP(forwardedFor string) *string {
	parts := strings.Split(forwardedFor, ",")
	if len(parts) == 0 {
		return nil
	}
	first := strings.TrimSpace(parts[0])
	ip := net.ParseIP(first)
	if ip == nil {
		return nil
	}
	parsedIP := ip.String()
	return &parsedIP
}

func parseForwardedScheme(forwardedProto string) *string {
	parts := strings.Split(forwardedProto, ",")
	if len(parts) == 0 {
		return nil
	}
	first := strings.ToLower(strings.TrimSpace(parts[0]))
	if first == "http" || first == "https" {
		return &first
	}
	return nil
}
