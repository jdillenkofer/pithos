package lua

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/textproto"
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/luahelper"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const authorizationFunctionName = "authorizeRequest"

var errAuthorizationFunctionNotFound = errors.New("authorization function " + authorizationFunctionName + " not found in Lua code")

type LuaAuthorizer struct {
	code                  string
	trustForwardedHeaders bool
	trustedProxyCIDRs     []*net.IPNet
	tracer                trace.Tracer
}

type Options struct {
	TrustForwardedHeaders bool
	TrustedProxyCIDRs     []string
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

func (authorizer *LuaAuthorizer) dryRun() error {
	_, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationPutObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	})
	return err
}

func NewLuaAuthorizer(code string) (*LuaAuthorizer, error) {
	return NewLuaAuthorizerWithOptions(code, Options{})
}

func NewLuaAuthorizerWithOptions(code string, options Options) (*LuaAuthorizer, error) {
	luaAuthorizer := &LuaAuthorizer{
		code:                  code,
		trustForwardedHeaders: options.TrustForwardedHeaders,
		trustedProxyCIDRs:     parseTrustedProxyCIDRs(options.TrustedProxyCIDRs),
		tracer:                otel.Tracer("internal/http/server/authorization/lua"),
	}
	err := luaAuthorizer.dryRun()
	if err != nil {
		return nil, err
	}
	return luaAuthorizer, nil
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

func stringInSlice(value string, values []string) bool {
	for _, currentValue := range values {
		if currentValue == value {
			return true
		}
	}
	return false
}

func luaStringSliceArg(L *lua.State, index int) ([]string, bool) {
	if !L.IsTable(index) {
		return nil, false
	}
	result := make([]string, 0)
	for i := 1; ; i++ {
		L.RawGetInt(index, i)
		if L.IsNil(-1) {
			L.Pop(1)
			break
		}
		value, ok := L.ToString(-1)
		L.Pop(1)
		if !ok {
			return nil, false
		}
		result = append(result, value)
	}
	return result, true
}

func ipInCIDR(ipStr string, cidr string) bool {
	ip := net.ParseIP(strings.TrimSpace(ipStr))
	if ip == nil {
		return false
	}
	_, ipNet, err := net.ParseCIDR(strings.TrimSpace(cidr))
	if err != nil {
		return false
	}
	return ipNet.Contains(ip)
}

func (authorizer *LuaAuthorizer) resolveClientIPAndScheme(httpRequest authorization.HTTPRequest) (*string, string) {
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

func (authorizer *LuaAuthorizer) AuthorizeRequest(ctx context.Context, request *authorization.Request) (bool, error) {
	return authorizer.callAuthorizerFunction(ctx, authorizationFunctionName, request)
}

func (authorizer *LuaAuthorizer) AuthorizeListBucket(ctx context.Context, request *authorization.Request, bucketName string) (bool, error) {
	return authorizer.callAuthorizerFunction(ctx, "authorizeListBucket", request, bucketName)
}

func (authorizer *LuaAuthorizer) AuthorizeListObject(ctx context.Context, request *authorization.Request, key string) (bool, error) {
	return authorizer.callAuthorizerFunction(ctx, "authorizeListObject", request, key)
}

func (authorizer *LuaAuthorizer) AuthorizeDeleteObjectEntry(ctx context.Context, request *authorization.Request, key string) (bool, error) {
	return authorizer.callAuthorizerFunction(ctx, "authorizeDeleteObjectEntry", request, key)
}

func (authorizer *LuaAuthorizer) AuthorizeListMultipartUpload(ctx context.Context, request *authorization.Request, key string, uploadID string) (bool, error) {
	return authorizer.callAuthorizerFunction(ctx, "authorizeListMultipartUpload", request, key, uploadID)
}

func (authorizer *LuaAuthorizer) AuthorizeListPart(ctx context.Context, request *authorization.Request, partNumber int32) (bool, error) {
	return authorizer.callAuthorizerFunction(ctx, "authorizeListPart", request, int(partNumber))
}

func (authorizer *LuaAuthorizer) callAuthorizerFunction(ctx context.Context, functionName string, request *authorization.Request, args ...interface{}) (bool, error) {
	_, span := authorizer.tracer.Start(ctx, "LuaAuthorizer.AuthorizeRequest")
	defer span.End()

	L := newLuaState()
	err := lua.DoString(L, authorizer.code)
	if err != nil {
		slog.ErrorContext(ctx, "Error while executing Lua code", "error", err)
		return false, err
	}
	L.Global(functionName)
	if !L.IsFunction(-1) {
		if functionName == authorizationFunctionName {
			slog.ErrorContext(ctx, "Authorization function not found in Lua code", "functionName", authorizationFunctionName)
			return false, errAuthorizationFunctionNotFound
		}
		return true, nil
	}
	authorizer.pushRequest(ctx, L, request)
	argCount := 1 + len(args)
	for _, arg := range args {
		luahelper.PushGoValue(L, arg)
	}
	err = L.ProtectedCall(argCount, 1, 0)
	if err != nil {
		slog.ErrorContext(ctx, "Error while calling authorization function", "error", err)
		return false, err
	}
	res := L.ToBoolean(1)
	L.Pop(1)
	slog.DebugContext(ctx, "Authorization result", "operation", request.Operation, "isAuthorized", res)
	return res, nil
}

func isReadOnly(operation string) bool {
	var isReadOnly bool
	switch operation {
	case authorization.OperationListBuckets, authorization.OperationHeadBucket, authorization.OperationHeadObject, authorization.OperationHeadObjectVersion, authorization.OperationListMultipartUploads, authorization.OperationListObjects, authorization.OperationListParts, authorization.OperationGetObject, authorization.OperationGetObjectVersion, authorization.OperationGetBucketWebsite, authorization.OperationGetBucketCORS, authorization.OperationGetBucketNotification, authorization.OperationGetObjectTagging, authorization.OperationGetObjectVersionTagging:
		isReadOnly = true
	case authorization.OperationCreateBucket, authorization.OperationDeleteBucket, authorization.OperationCreateMultipartUpload, authorization.OperationCompleteMultipartUpload, authorization.OperationUploadPart, authorization.OperationUploadPartCopy, authorization.OperationPutObject, authorization.OperationCopyObject, authorization.OperationAppendObject, authorization.OperationAbortMultipartUpload, authorization.OperationDeleteObject, authorization.OperationDeleteObjectVersion, authorization.OperationDeleteObjects, authorization.OperationPutBucketWebsite, authorization.OperationDeleteBucketWebsite, authorization.OperationPutBucketCORS, authorization.OperationDeleteBucketCORS, authorization.OperationPutBucketNotification, authorization.OperationPutObjectTagging, authorization.OperationPutObjectVersionTagging, authorization.OperationDeleteObjectTagging, authorization.OperationDeleteObjectVersionTagging:
		isReadOnly = false
	}
	return isReadOnly
}

func (authorizer *LuaAuthorizer) pushRequest(ctx context.Context, L *lua.State, request *authorization.Request) {
	clientIP, scheme := authorizer.resolveClientIPAndScheme(request.HttpRequest)
	request.HttpRequest.ClientIP = clientIP
	request.HttpRequest.Scheme = scheme

	luahelper.PushGoValue(L, request)
	L.Field(-1, "httpRequest")
	if L.IsTable(-1) {
		L.PushGoFunction(func(L *lua.State) int {
			expectedMethod, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.Field(1, "method")
			method, _ := L.ToString(-1)
			L.PushBoolean(strings.EqualFold(method, expectedMethod))
			return 1
		})
		L.SetField(-2, "isMethod")
		L.PushGoFunction(func(L *lua.State) int {
			headerName, ok := L.ToString(2)
			if !ok {
				L.PushNil()
				return 1
			}
			headerValues := getHeaderValuesCaseInsensitive(request.HttpRequest.Headers, headerName)
			if len(headerValues) == 0 {
				L.PushNil()
				return 1
			}
			L.PushString(headerValues[0])
			return 1
		})
		L.SetField(-2, "header")
		L.PushGoFunction(func(L *lua.State) int {
			headerName, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			headerValues := getHeaderValuesCaseInsensitive(request.HttpRequest.Headers, headerName)
			L.PushBoolean(len(headerValues) > 0)
			return 1
		})
		L.SetField(-2, "hasHeader")
		L.PushGoFunction(func(L *lua.State) int {
			headerName, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			expectedValue, ok := L.ToString(3)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			headerValues := getHeaderValuesCaseInsensitive(request.HttpRequest.Headers, headerName)
			for _, headerValue := range headerValues {
				if headerValue == expectedValue {
					L.PushBoolean(true)
					return 1
				}
			}
			L.PushBoolean(false)
			return 1
		})
		L.SetField(-2, "headerEquals")
		L.PushGoFunction(func(L *lua.State) int {
			paramName, ok := L.ToString(2)
			if !ok {
				L.PushNil()
				return 1
			}
			paramValues := request.HttpRequest.QueryParams[paramName]
			if len(paramValues) == 0 {
				L.PushNil()
				return 1
			}
			L.PushString(paramValues[0])
			return 1
		})
		L.SetField(-2, "queryParam")
		L.PushGoFunction(func(L *lua.State) int {
			paramName, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			paramValues := request.HttpRequest.QueryParams[paramName]
			L.PushBoolean(len(paramValues) > 0)
			return 1
		})
		L.SetField(-2, "hasQueryParam")
		L.PushGoFunction(func(L *lua.State) int {
			paramName, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			expectedValue, ok := L.ToString(3)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			queryParamValues := request.HttpRequest.QueryParams[paramName]
			for _, queryParamValue := range queryParamValues {
				if queryParamValue == expectedValue {
					L.PushBoolean(true)
					return 1
				}
			}
			L.PushBoolean(false)
			return 1
		})
		L.SetField(-2, "queryParamEquals")
		L.PushGoFunction(func(L *lua.State) int {
			expectedPath, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(request.HttpRequest.Path == expectedPath)
			return 1
		})
		L.SetField(-2, "pathEquals")
		L.PushGoFunction(func(L *lua.State) int {
			prefix, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.HasPrefix(request.HttpRequest.Path, prefix))
			return 1
		})
		L.SetField(-2, "pathHasPrefix")
		L.PushGoFunction(func(L *lua.State) int {
			expectedHost, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.EqualFold(request.HttpRequest.Host, expectedHost))
			return 1
		})
		L.SetField(-2, "hostEquals")
		L.PushGoFunction(func(L *lua.State) int {
			suffix, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.HasSuffix(strings.ToLower(request.HttpRequest.Host), strings.ToLower(suffix)))
			return 1
		})
		L.SetField(-2, "hostHasSuffix")
		L.PushGoFunction(func(L *lua.State) int {
			expectedScheme, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.EqualFold(request.HttpRequest.Scheme, expectedScheme))
			return 1
		})
		L.SetField(-2, "isScheme")
		L.PushGoFunction(func(L *lua.State) int {
			expectedProto, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.EqualFold(request.HttpRequest.Proto, expectedProto))
			return 1
		})
		L.SetField(-2, "isProto")
		L.PushGoFunction(func(L *lua.State) int {
			cidr, ok := L.ToString(2)
			if !ok || request.HttpRequest.ClientIP == nil {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(ipInCIDR(*request.HttpRequest.ClientIP, cidr))
			return 1
		})
		L.SetField(-2, "clientIPInCIDR")
		L.PushGoFunction(func(L *lua.State) int {
			cidrs, ok := luaStringSliceArg(L, 2)
			if !ok || request.HttpRequest.ClientIP == nil {
				L.PushBoolean(false)
				return 1
			}
			for _, cidr := range cidrs {
				if ipInCIDR(*request.HttpRequest.ClientIP, cidr) {
					L.PushBoolean(true)
					return 1
				}
			}
			L.PushBoolean(false)
			return 1
		})
		L.SetField(-2, "clientIPInCIDRs")
		L.PushGoFunction(func(L *lua.State) int {
			cidr, ok := L.ToString(2)
			if !ok || request.HttpRequest.RemoteIP == nil {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(ipInCIDR(*request.HttpRequest.RemoteIP, cidr))
			return 1
		})
		L.SetField(-2, "remoteIPInCIDR")
	}
	L.Pop(1)
	L.PushGoFunction(func(L *lua.State) int {
		L.Field(1, "operation")
		operation, _ := L.ToString(-1)
		isReadOnly := isReadOnly(operation)
		L.PushBoolean(isReadOnly)
		return 1
	})
	L.SetField(-2, "isReadOnly")
	L.PushGoFunction(func(L *lua.State) int {
		L.Field(1, "operation")
		operation, _ := L.ToString(-1)
		L.PushBoolean(!isReadOnly(operation))
		return 1
	})
	L.SetField(-2, "isWriteOperation")
	L.PushGoFunction(func(L *lua.State) int {
		expectedOperation, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.Field(1, "operation")
		operation, _ := L.ToString(-1)
		L.PushBoolean(operation == expectedOperation)
		return 1
	})
	L.SetField(-2, "isOperation")
	L.PushGoFunction(func(L *lua.State) int {
		expectedOperations, ok := luaStringSliceArg(L, 2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.Field(1, "operation")
		operation, _ := L.ToString(-1)
		L.PushBoolean(stringInSlice(operation, expectedOperations))
		return 1
	})
	L.SetField(-2, "isOperationIn")
	L.PushGoFunction(func(L *lua.State) int {
		L.Field(1, "authorization")
		L.Field(-1, "accessKeyId")
		isAnonymous := L.IsNil(-1)
		L.PushBoolean(isAnonymous)
		return 1
	})
	L.SetField(-2, "isAnonymous")
	L.PushGoFunction(func(L *lua.State) int {
		L.Field(1, "authorization")
		L.Field(-1, "accessKeyId")
		hasAccessKeyId := !L.IsNil(-1)
		L.PushBoolean(hasAccessKeyId)
		return 1
	})
	L.SetField(-2, "hasAccessKeyId")
	L.PushGoFunction(func(L *lua.State) int {
		expectedAccessKeyId, ok := L.ToString(2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.Field(1, "authorization")
		L.Field(-1, "accessKeyId")
		accessKeyId, ok := L.ToString(-1)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(accessKeyId == expectedAccessKeyId)
		return 1
	})
	L.SetField(-2, "accessKeyIdEquals")
	L.PushGoFunction(func(L *lua.State) int {
		expectedAccessKeyIds, ok := luaStringSliceArg(L, 2)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.Field(1, "authorization")
		L.Field(-1, "accessKeyId")
		accessKeyId, ok := L.ToString(-1)
		if !ok {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(stringInSlice(accessKeyId, expectedAccessKeyIds))
		return 1
	})
	L.SetField(-2, "accessKeyIdIn")
	L.PushGoFunction(func(L *lua.State) int {
		expectedBucket, ok := L.ToString(2)
		if !ok || request.Bucket == nil {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(*request.Bucket == expectedBucket)
		return 1
	})
	L.SetField(-2, "bucketEquals")
	L.PushGoFunction(func(L *lua.State) int {
		prefix, ok := L.ToString(2)
		if !ok || request.Key == nil {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(strings.HasPrefix(*request.Key, prefix))
		return 1
	})
	L.SetField(-2, "keyHasPrefix")
	L.PushGoFunction(func(L *lua.State) int {
		suffix, ok := L.ToString(2)
		if !ok || request.Key == nil {
			L.PushBoolean(false)
			return 1
		}
		L.PushBoolean(strings.HasSuffix(*request.Key, suffix))
		return 1
	})
	L.SetField(-2, "keyHasSuffix")

	// makeLazyTagLoader returns a memoized loader over a tag resolver so multiple
	// tag predicates in one policy evaluation cause at most one storage lookup.
	// A nil resolver yields an empty tag set; a resolver error is surfaced to the
	// caller, which raises a Lua error so the whole authorization fails closed.
	makeLazyTagLoader := func(resolve func(ctx context.Context) (map[string]string, error)) func() (map[string]string, error) {
		var cached map[string]string
		loaded := false
		return func() (map[string]string, error) {
			if loaded {
				return cached, nil
			}
			if resolve == nil {
				cached = map[string]string{}
				loaded = true
				return cached, nil
			}
			tags, err := resolve(ctx)
			if err != nil {
				return nil, err
			}
			if tags == nil {
				tags = map[string]string{}
			}
			cached = tags
			loaded = true
			return cached, nil
		}
	}

	// registerTagPredicates installs a family of tag predicates on the request
	// table: <tagsName>() returns the whole tag set as a table, <tagName>(key)
	// returns a single value or nil, <equalsName>(key, value) and <hasName>(key)
	// are boolean checks. A loader error raises a Lua error (fail closed).
	registerTagPredicates := func(tagsName, tagName, equalsName, hasName string, load func() (map[string]string, error)) {
		loadOrError := func(L *lua.State) (map[string]string, bool) {
			tags, err := load()
			if err != nil {
				lua.Errorf(L, "failed to resolve %s: %v", tagsName, err)
				return nil, false
			}
			return tags, true
		}
		L.PushGoFunction(func(L *lua.State) int {
			tags, ok := loadOrError(L)
			if !ok {
				return 0
			}
			L.NewTable()
			for k, v := range tags {
				L.PushString(v)
				L.SetField(-2, k)
			}
			return 1
		})
		L.SetField(-2, tagsName)
		L.PushGoFunction(func(L *lua.State) int {
			key, ok := L.ToString(2)
			if !ok {
				L.PushNil()
				return 1
			}
			tags, okLoad := loadOrError(L)
			if !okLoad {
				return 0
			}
			if value, exists := tags[key]; exists {
				L.PushString(value)
			} else {
				L.PushNil()
			}
			return 1
		})
		L.SetField(-2, tagName)
		L.PushGoFunction(func(L *lua.State) int {
			key, ok := L.ToString(2)
			expectedValue, ok2 := L.ToString(3)
			if !ok || !ok2 {
				L.PushBoolean(false)
				return 1
			}
			tags, okLoad := loadOrError(L)
			if !okLoad {
				return 0
			}
			value, exists := tags[key]
			L.PushBoolean(exists && value == expectedValue)
			return 1
		})
		L.SetField(-2, equalsName)
		L.PushGoFunction(func(L *lua.State) int {
			key, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			tags, okLoad := loadOrError(L)
			if !okLoad {
				return 0
			}
			_, exists := tags[key]
			L.PushBoolean(exists)
			return 1
		})
		L.SetField(-2, hasName)
	}

	// Existing object tags (s3:ExistingObjectTag) — the tags currently stored on
	// the request's target object, resolved lazily.
	registerTagPredicates("objectTags", "objectTag", "objectTagEquals", "hasObjectTag",
		makeLazyTagLoader(request.ResolveExistingObjectTags))
	// Existing tags on the copy source object (CopyObject/UploadPartCopy) — AWS
	// evaluates s3:ExistingObjectTag against the source for the copy's read side.
	registerTagPredicates("sourceObjectTags", "sourceObjectTag", "sourceObjectTagEquals", "hasSourceObjectTag",
		makeLazyTagLoader(request.ResolveExistingSourceObjectTags))
	// Request tags (s3:RequestObjectTag) — the tags supplied in the request
	// itself; already in memory, no resolver needed.
	registerTagPredicates("requestTags", "requestTag", "requestTagEquals", "hasRequestTag",
		func() (map[string]string, error) {
			return request.RequestObjectTags, nil
		})
}

func newLuaState() *lua.State {
	L := lua.NewState()
	lua.Require(L, "_G", lua.BaseOpen, true)
	L.Pop(1)
	lua.Require(L, "table", lua.TableOpen, true)
	L.Pop(1)
	lua.Require(L, "string", lua.StringOpen, true)
	L.Pop(1)
	lua.Require(L, "math", lua.MathOpen, true)
	L.Pop(1)
	return L
}
