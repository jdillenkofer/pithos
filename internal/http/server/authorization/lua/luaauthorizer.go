package lua

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/textproto"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/go-lua"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const authorizationFunctionName = "authorizeRequest"

var errAuthorizationFunctionNotFound = errors.New("authorization function " + authorizationFunctionName + " not found in Lua code")
var errAuthorizationResultNotBoolean = errors.New("authorization function " + authorizationFunctionName + " must return a boolean")

type LuaAuthorizer struct {
	code string
	// chunk is the policy code precompiled to a Lua binary chunk. Loading it
	// skips lexing and parsing the source on every request; the chunk keeps
	// the source name and debug info, so runtime errors read the same.
	chunk         []byte
	proxyResolver *authorization.ProxyResolver
	tracer        trace.Tracer
}

type Options struct {
	TrustForwardedHeaders bool
	TrustedProxyCIDRs     []string
}

func (authorizer *LuaAuthorizer) dryRun() error {
	_, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationPutObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
			AuthType:    "REST-HEADER",
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
	proxyResolver, err := authorization.NewProxyResolver(authorization.ProxyOptions{
		TrustForwardedHeaders: options.TrustForwardedHeaders,
		TrustedProxyCIDRs:     options.TrustedProxyCIDRs,
	})
	if err != nil {
		return nil, err
	}
	chunk, err := compileChunk(code)
	if err != nil {
		return nil, err
	}
	luaAuthorizer := &LuaAuthorizer{
		code:          code,
		chunk:         chunk,
		proxyResolver: proxyResolver,
		tracer:        otel.Tracer("internal/http/server/authorization/lua"),
	}
	err = luaAuthorizer.dryRun()
	if err != nil {
		return nil, err
	}
	return luaAuthorizer, nil
}

func compileChunk(code string) ([]byte, error) {
	L := lua.NewState()
	if err := lua.LoadString(L, code); err != nil {
		return nil, err
	}
	var chunk bytes.Buffer
	if err := L.Dump(&chunk); err != nil {
		return nil, err
	}
	return chunk.Bytes(), nil
}

// unsafeBaseFunctions are base library functions a policy must not reach:
// dofile and loadfile read and execute files from disk, and load compiles
// arbitrary chunks at runtime, including unverified binary chunks.
var unsafeBaseFunctions = []string{"dofile", "loadfile", "load"}

func removeUnsafeBaseFunctions(L *lua.State) {
	for _, name := range unsafeBaseFunctions {
		L.PushNil()
		L.SetGlobal(name)
	}
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
	return authorizer.proxyResolver.Resolve(httpRequest)
}

func pushNullableString(L *lua.State, str *string) {
	if str != nil {
		L.PushString(*str)
	} else {
		L.PushNil()
	}
}

func pushNullableInt(L *lua.State, num *int) {
	if num != nil {
		L.PushInteger(*num)
	} else {
		L.PushNil()
	}
}

func pushNullableNumber(L *lua.State, num *float64) {
	if num != nil {
		L.PushNumber(*num)
	} else {
		L.PushNil()
	}
}

func pushNullableBoolean(L *lua.State, b *bool) {
	if b != nil {
		L.PushBoolean(*b)
	} else {
		L.PushNil()
	}
}

// luaFieldNames caches the Lua field names (lower camel case) of struct
// types pushed by pushGoType.
var luaFieldNames sync.Map

func luaFieldNamesOf(t reflect.Type) []string {
	if names, ok := luaFieldNames.Load(t); ok {
		return names.([]string)
	}
	names := make([]string, t.NumField())
	for i := range names {
		name := t.Field(i).Name
		names[i] = strings.ToLower(name[:1]) + name[1:]
	}
	luaFieldNames.Store(t, names)
	return names
}

func pushGoType(L *lua.State, obj interface{}) {
	if obj == nil {
		L.PushNil()
		return
	}
	switch v := obj.(type) {
	case string:
		L.PushString(v)
	case *string:
		pushNullableString(L, v)
	case int:
		L.PushInteger(v)
	case *int:
		pushNullableInt(L, v)
	case float64:
		L.PushNumber(v)
	case *float64:
		pushNullableNumber(L, v)
	case bool:
		L.PushBoolean(v)
	case *bool:
		pushNullableBoolean(L, v)
	case time.Duration:
		L.PushInteger(int(v))
	default:
		t := reflect.TypeOf(v)

		// If it's a pointer, get the underlying type
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}

		if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
			L.NewTable()
			val := reflect.ValueOf(v)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			for i := 0; i < val.Len(); i++ {
				item := val.Index(i)
				if item.CanInterface() {
					pushGoType(L, item.Interface())
					L.RawSetInt(-2, i+1) // Lua arrays are 1-indexed
				}
			}
			// Put the length of the array at index 0
			L.PushInteger(val.Len())
			L.RawSetInt(-2, 0)
		} else if t.Kind() == reflect.Map {
			L.NewTable()
			val := reflect.ValueOf(v)
			for _, key := range val.MapKeys() {
				value := val.MapIndex(key)
				if key.CanInterface() && value.CanInterface() {
					pushGoType(L, key.Interface())
					pushGoType(L, value.Interface())
					L.SetTable(-3)
				}
			}
		} else if t.Kind() == reflect.Struct {
			L.NewTable()
			val := reflect.ValueOf(v)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			fieldNames := luaFieldNamesOf(t)
			for i := 0; i < t.NumField(); i++ {
				fieldValue := val.Field(i)
				if fieldValue.CanInterface() {
					pushGoType(L, fieldValue.Interface())
					L.SetField(-2, fieldNames[i])
				}
			}
		} else {
			L.PushNil() // Unsupported type
		}
	}
}

func (authorizer *LuaAuthorizer) AuthorizeRequest(ctx context.Context, request *authorization.Request) (authorization.Decision, error) {
	started := time.Now()
	effect, err := authorizer.callAuthorizerFunction(ctx, authorizationFunctionName, request)
	authorization.ObserveDecision("lua", effect, started)
	return authorization.Decision{Effect: effect}, err
}

func (authorizer *LuaAuthorizer) callAuthorizerFunction(ctx context.Context, functionName string, request *authorization.Request, args ...interface{}) (authorization.Effect, error) {
	_, span := authorizer.tracer.Start(ctx, "LuaAuthorizer.AuthorizeRequest")
	defer span.End()

	L := lua.NewState()
	// Load only necessary libraries
	lua.Require(L, "_G", lua.BaseOpen, true)
	L.Pop(1)
	lua.Require(L, "table", lua.TableOpen, true)
	L.Pop(1)
	lua.Require(L, "string", lua.StringOpen, true)
	L.Pop(1)
	lua.Require(L, "math", lua.MathOpen, true)
	L.Pop(1)
	removeUnsafeBaseFunctions(L)
	err := L.Load(bytes.NewReader(authorizer.chunk), authorizer.code, "b")
	if err == nil {
		err = L.ProtectedCall(0, lua.MultipleReturns, 0)
	}
	if err != nil {
		slog.ErrorContext(ctx, "Error while executing Lua code", "error", err)
		return authorization.ImplicitDeny, err
	}
	// Discard top-level chunk results. Only the function's return value is an
	// authorization decision.
	L.SetTop(0)
	L.Global(functionName)
	if !L.IsFunction(-1) {
		if functionName == authorizationFunctionName {
			slog.ErrorContext(ctx, "Authorization function not found in Lua code", "functionName", authorizationFunctionName)
			return authorization.ImplicitDeny, errAuthorizationFunctionNotFound
		}
		return authorization.Allow, nil
	}
	authorizer.pushRequest(ctx, L, request)
	argCount := 1 + len(args)
	for _, arg := range args {
		pushGoType(L, arg)
	}
	err = L.ProtectedCall(argCount, 1, 0)
	if err != nil {
		slog.ErrorContext(ctx, "Error while calling authorization function", "error", err)
		return authorization.ImplicitDeny, err
	}
	if !L.IsBoolean(-1) {
		return authorization.ImplicitDeny, errAuthorizationResultNotBoolean
	}
	allowed := L.ToBoolean(-1)
	effect := authorization.ExplicitDeny
	if allowed {
		effect = authorization.Allow
	}
	L.Pop(1)
	slog.DebugContext(ctx, "Authorization result", "operation", request.Operation, "effect", effect)
	return effect, nil
}

func isReadOnly(operation string) bool {
	var isReadOnly bool
	switch operation {
	case authorization.OperationListBuckets, authorization.OperationHeadBucket, authorization.OperationHeadObject, authorization.OperationHeadObjectVersion, authorization.OperationListMultipartUploads, authorization.OperationListObjects, authorization.OperationListParts, authorization.OperationGetObject, authorization.OperationGetObjectVersion, authorization.OperationGetBucketWebsite, authorization.OperationGetBucketCORS, authorization.OperationGetBucketNotification, authorization.OperationGetBucketTagging, authorization.OperationGetBucketLifecycle, authorization.OperationGetObjectTagging, authorization.OperationGetObjectVersionTagging:
		isReadOnly = true
	case authorization.OperationCreateBucket, authorization.OperationDeleteBucket, authorization.OperationCreateMultipartUpload, authorization.OperationCompleteMultipartUpload, authorization.OperationUploadPart, authorization.OperationUploadPartCopy, authorization.OperationPutObject, authorization.OperationCopyObject, authorization.OperationAppendObject, authorization.OperationAbortMultipartUpload, authorization.OperationDeleteObject, authorization.OperationDeleteObjectVersion, authorization.OperationDeleteObjects, authorization.OperationPutBucketWebsite, authorization.OperationDeleteBucketWebsite, authorization.OperationPutBucketCORS, authorization.OperationDeleteBucketCORS, authorization.OperationPutBucketNotification, authorization.OperationPutBucketTagging, authorization.OperationDeleteBucketTagging, authorization.OperationPutBucketLifecycle, authorization.OperationDeleteBucketLifecycle, authorization.OperationPutObjectTagging, authorization.OperationPutObjectVersionTagging, authorization.OperationDeleteObjectTagging, authorization.OperationDeleteObjectVersionTagging:
		isReadOnly = false
	}
	return isReadOnly
}

// lazyTags is a memoized loader over a tag resolver so multiple tag predicates
// in one policy evaluation cause at most one storage lookup. A nil resolver
// yields an empty tag set; a resolver error is surfaced to the caller, which
// raises a Lua error so the whole authorization fails closed.
type lazyTags struct {
	resolve func(ctx context.Context) (map[string]string, error)
	cached  map[string]string
	loaded  bool
}

func (t *lazyTags) load(ctx context.Context) (map[string]string, error) {
	if t.loaded {
		return t.cached, nil
	}
	if t.resolve == nil {
		t.cached = map[string]string{}
		t.loaded = true
		return t.cached, nil
	}
	tags, err := t.resolve(ctx)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		tags = map[string]string{}
	}
	t.cached = tags
	t.loaded = true
	return t.cached, nil
}

// requestMethodContext is the per-request state shared by the methods
// exposed on the Lua request table.
type requestMethodContext struct {
	ctx              context.Context
	request          *authorization.Request
	objectTags       lazyTags
	sourceObjectTags lazyTags
	requestTags      lazyTags
}

type methodFactory func(m *requestMethodContext) lua.Function

// pushLazyMethods sets a metatable on the table at the top of the stack whose
// __index creates a method on first access and caches it on the table.
// Policies typically call only a handful of the available methods, so building
// every closure eagerly per request would be wasted work.
func pushLazyMethods(L *lua.State, methods map[string]methodFactory, m *requestMethodContext) {
	L.NewTable()
	L.PushGoFunction(func(L *lua.State) int {
		if L.TypeOf(2) != lua.TypeString {
			L.PushNil()
			return 1
		}
		name, _ := L.ToString(2)
		factory, ok := methods[name]
		if !ok {
			L.PushNil()
			return 1
		}
		L.PushGoFunction(factory(m))
		L.PushString(name)
		L.PushValue(-2)
		L.RawSet(1)
		return 1
	})
	L.SetField(-2, "__index")
	L.SetMetaTable(-2)
}

var httpRequestMethods = map[string]methodFactory{
	"isMethod": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			expectedMethod, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.Field(1, "method")
			method, _ := L.ToString(-1)
			L.PushBoolean(strings.EqualFold(method, expectedMethod))
			return 1
		}
	},
	"header": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			headerName, ok := L.ToString(2)
			if !ok {
				L.PushNil()
				return 1
			}
			headerValues := getHeaderValuesCaseInsensitive(m.request.HttpRequest.Headers, headerName)
			if len(headerValues) == 0 {
				L.PushNil()
				return 1
			}
			L.PushString(headerValues[0])
			return 1
		}
	},
	"hasHeader": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			headerName, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			headerValues := getHeaderValuesCaseInsensitive(m.request.HttpRequest.Headers, headerName)
			L.PushBoolean(len(headerValues) > 0)
			return 1
		}
	},
	"headerEquals": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
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
			headerValues := getHeaderValuesCaseInsensitive(m.request.HttpRequest.Headers, headerName)
			L.PushBoolean(stringInSlice(expectedValue, headerValues))
			return 1
		}
	},
	"queryParam": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			paramName, ok := L.ToString(2)
			if !ok {
				L.PushNil()
				return 1
			}
			paramValues := m.request.HttpRequest.QueryParams[paramName]
			if len(paramValues) == 0 {
				L.PushNil()
				return 1
			}
			L.PushString(paramValues[0])
			return 1
		}
	},
	"hasQueryParam": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			paramName, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			paramValues := m.request.HttpRequest.QueryParams[paramName]
			L.PushBoolean(len(paramValues) > 0)
			return 1
		}
	},
	"queryParamEquals": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
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
			L.PushBoolean(stringInSlice(expectedValue, m.request.HttpRequest.QueryParams[paramName]))
			return 1
		}
	},
	"pathEquals": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			expectedPath, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(m.request.HttpRequest.Path == expectedPath)
			return 1
		}
	},
	"pathHasPrefix": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			prefix, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.HasPrefix(m.request.HttpRequest.Path, prefix))
			return 1
		}
	},
	"hostEquals": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			expectedHost, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.EqualFold(m.request.HttpRequest.Host, expectedHost))
			return 1
		}
	},
	"hostHasSuffix": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			suffix, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.HasSuffix(strings.ToLower(m.request.HttpRequest.Host), strings.ToLower(suffix)))
			return 1
		}
	},
	"isScheme": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			expectedScheme, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.EqualFold(m.request.HttpRequest.Scheme, expectedScheme))
			return 1
		}
	},
	"isProto": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			expectedProto, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(strings.EqualFold(m.request.HttpRequest.Proto, expectedProto))
			return 1
		}
	},
	"clientIPInCIDR": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			cidr, ok := L.ToString(2)
			if !ok || m.request.HttpRequest.ClientIP == nil {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(ipInCIDR(*m.request.HttpRequest.ClientIP, cidr))
			return 1
		}
	},
	"clientIPInCIDRs": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			cidrs, ok := luaStringSliceArg(L, 2)
			if !ok || m.request.HttpRequest.ClientIP == nil {
				L.PushBoolean(false)
				return 1
			}
			for _, cidr := range cidrs {
				if ipInCIDR(*m.request.HttpRequest.ClientIP, cidr) {
					L.PushBoolean(true)
					return 1
				}
			}
			L.PushBoolean(false)
			return 1
		}
	},
	"remoteIPInCIDR": func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			cidr, ok := L.ToString(2)
			if !ok || m.request.HttpRequest.RemoteIP == nil {
				L.PushBoolean(false)
				return 1
			}
			L.PushBoolean(ipInCIDR(*m.request.HttpRequest.RemoteIP, cidr))
			return 1
		}
	},
}

func pushAuthorizationStringEquals(field string) methodFactory {
	return func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			expected, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.Field(1, "authorization")
			L.Field(-1, field)
			actual, ok := L.ToString(-1)
			L.PushBoolean(ok && actual == expected)
			return 1
		}
	}
}

func pushAuthorizationStringIn(field string) methodFactory {
	return func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			expected, ok := luaStringSliceArg(L, 2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			L.Field(1, "authorization")
			L.Field(-1, field)
			actual, ok := L.ToString(-1)
			L.PushBoolean(ok && stringInSlice(actual, expected))
			return 1
		}
	}
}

func pushAuthorizationFieldPresent(field string, present bool) methodFactory {
	return func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			L.Field(1, "authorization")
			L.Field(-1, field)
			L.PushBoolean(L.IsNil(-1) != present)
			return 1
		}
	}
}

// addTagPredicates installs a family of tag predicates: <tagsName>() returns
// the whole tag set as a table, <tagName>(key) returns a single value or nil,
// <equalsName>(key, value) and <hasName>(key) are boolean checks. A loader
// error raises a Lua error (fail closed).
func addTagPredicates(methods map[string]methodFactory, tagsName, tagName, equalsName, hasName string, tags func(m *requestMethodContext) *lazyTags) {
	loadOrError := func(L *lua.State, m *requestMethodContext) (map[string]string, bool) {
		loaded, err := tags(m).load(m.ctx)
		if err != nil {
			lua.Errorf(L, "failed to resolve %s: %v", tagsName, err)
			return nil, false
		}
		return loaded, true
	}
	methods[tagsName] = func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			tags, ok := loadOrError(L, m)
			if !ok {
				return 0
			}
			L.NewTable()
			for k, v := range tags {
				L.PushString(v)
				L.SetField(-2, k)
			}
			return 1
		}
	}
	methods[tagName] = func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			key, ok := L.ToString(2)
			if !ok {
				L.PushNil()
				return 1
			}
			tags, okLoad := loadOrError(L, m)
			if !okLoad {
				return 0
			}
			if value, exists := tags[key]; exists {
				L.PushString(value)
			} else {
				L.PushNil()
			}
			return 1
		}
	}
	methods[equalsName] = func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			key, ok := L.ToString(2)
			expectedValue, ok2 := L.ToString(3)
			if !ok || !ok2 {
				L.PushBoolean(false)
				return 1
			}
			tags, okLoad := loadOrError(L, m)
			if !okLoad {
				return 0
			}
			value, exists := tags[key]
			L.PushBoolean(exists && value == expectedValue)
			return 1
		}
	}
	methods[hasName] = func(m *requestMethodContext) lua.Function {
		return func(L *lua.State) int {
			key, ok := L.ToString(2)
			if !ok {
				L.PushBoolean(false)
				return 1
			}
			tags, okLoad := loadOrError(L, m)
			if !okLoad {
				return 0
			}
			_, exists := tags[key]
			L.PushBoolean(exists)
			return 1
		}
	}
}

var requestMethods = func() map[string]methodFactory {
	methods := map[string]methodFactory{
		"isReadOnly": func(m *requestMethodContext) lua.Function {
			return func(L *lua.State) int {
				L.Field(1, "operation")
				operation, _ := L.ToString(-1)
				L.PushBoolean(isReadOnly(operation))
				return 1
			}
		},
		"isWriteOperation": func(m *requestMethodContext) lua.Function {
			return func(L *lua.State) int {
				L.Field(1, "operation")
				operation, _ := L.ToString(-1)
				L.PushBoolean(!isReadOnly(operation))
				return 1
			}
		},
		"isOperation": func(m *requestMethodContext) lua.Function {
			return func(L *lua.State) int {
				expectedOperation, ok := L.ToString(2)
				if !ok {
					L.PushBoolean(false)
					return 1
				}
				L.Field(1, "operation")
				operation, _ := L.ToString(-1)
				L.PushBoolean(operation == expectedOperation)
				return 1
			}
		},
		"isOperationIn": func(m *requestMethodContext) lua.Function {
			return func(L *lua.State) int {
				expectedOperations, ok := luaStringSliceArg(L, 2)
				if !ok {
					L.PushBoolean(false)
					return 1
				}
				L.Field(1, "operation")
				operation, _ := L.ToString(-1)
				L.PushBoolean(stringInSlice(operation, expectedOperations))
				return 1
			}
		},
		"isAnonymous":       pushAuthorizationFieldPresent("accessKeyId", false),
		"hasAccessKeyId":    pushAuthorizationFieldPresent("accessKeyId", true),
		"accessKeyIdEquals": pushAuthorizationStringEquals("accessKeyId"),
		"accessKeyIdIn":     pushAuthorizationStringIn("accessKeyId"),
		"hasPrincipalId":    pushAuthorizationFieldPresent("principalId", true),
		"principalIdEquals": pushAuthorizationStringEquals("principalId"),
		"principalIdIn":     pushAuthorizationStringIn("principalId"),
		"bucketEquals": func(m *requestMethodContext) lua.Function {
			return func(L *lua.State) int {
				expectedBucket, ok := L.ToString(2)
				if !ok || m.request.Bucket == nil {
					L.PushBoolean(false)
					return 1
				}
				L.PushBoolean(*m.request.Bucket == expectedBucket)
				return 1
			}
		},
		"keyHasPrefix": func(m *requestMethodContext) lua.Function {
			return func(L *lua.State) int {
				prefix, ok := L.ToString(2)
				if !ok || m.request.Key == nil {
					L.PushBoolean(false)
					return 1
				}
				L.PushBoolean(strings.HasPrefix(*m.request.Key, prefix))
				return 1
			}
		},
		"keyHasSuffix": func(m *requestMethodContext) lua.Function {
			return func(L *lua.State) int {
				suffix, ok := L.ToString(2)
				if !ok || m.request.Key == nil {
					L.PushBoolean(false)
					return 1
				}
				L.PushBoolean(strings.HasSuffix(*m.request.Key, suffix))
				return 1
			}
		},
	}
	// Existing object tags (s3:ExistingObjectTag) — the tags currently stored on
	// the request's target object, resolved lazily.
	addTagPredicates(methods, "objectTags", "objectTag", "objectTagEquals", "hasObjectTag",
		func(m *requestMethodContext) *lazyTags { return &m.objectTags })
	// Existing tags on the copy source object (CopyObject/UploadPartCopy) — AWS
	// evaluates s3:ExistingObjectTag against the source for the copy's read side.
	addTagPredicates(methods, "sourceObjectTags", "sourceObjectTag", "sourceObjectTagEquals", "hasSourceObjectTag",
		func(m *requestMethodContext) *lazyTags { return &m.sourceObjectTags })
	// Request tags (s3:RequestObjectTag) — the tags supplied in the request
	// itself; already in memory, no resolver needed.
	addTagPredicates(methods, "requestTags", "requestTag", "requestTagEquals", "hasRequestTag",
		func(m *requestMethodContext) *lazyTags { return &m.requestTags })
	return methods
}()

func (authorizer *LuaAuthorizer) pushRequest(ctx context.Context, L *lua.State, request *authorization.Request) {
	clientIP, scheme := authorizer.resolveClientIPAndScheme(request.HttpRequest)
	request.HttpRequest.ClientIP = clientIP
	request.HttpRequest.Scheme = scheme

	m := &requestMethodContext{
		ctx:              ctx,
		request:          request,
		objectTags:       lazyTags{resolve: request.ResolveExistingObjectTags},
		sourceObjectTags: lazyTags{resolve: request.ResolveExistingSourceObjectTags},
		requestTags: lazyTags{resolve: func(context.Context) (map[string]string, error) {
			return request.RequestObjectTags, nil
		}},
	}

	pushGoType(L, request)
	L.Field(-1, "httpRequest")
	if L.IsTable(-1) {
		pushLazyMethods(L, httpRequestMethods, m)
	}
	L.Pop(1)
	pushLazyMethods(L, requestMethods, m)
}
