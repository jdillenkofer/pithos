package lua

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/Shopify/go-lua"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const authorizationFunctionName = "authorizeRequest"

var errAuthorizationFunctionNotFound = errors.New("authorization function " + authorizationFunctionName + " not found in Lua code")

type LuaAuthorizer struct {
	code   string
	tracer trace.Tracer
}

func (authorizer *LuaAuthorizer) dryRun() error {
	_, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationPutObject,
		Authorization: authorization.Authorization{
			AccessKeyId: "AKIAIOSFODNN7EXAMPLE",
		},
		Bucket: nil,
		Key:    nil,
	})
	return err
}

func NewLuaAuthorizer(code string) (*LuaAuthorizer, error) {
	luaAuthorizer := &LuaAuthorizer{
		code:   code,
		tracer: otel.Tracer("internal/http/server/authorization/lua"),
	}
	err := luaAuthorizer.dryRun()
	if err != nil {
		return nil, err
	}
	return luaAuthorizer, nil
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

func pushGoType(L *lua.State, obj interface{}) {
	if obj == nil {
		L.PushNil()
		return
	}
	switch v := obj.(type) {
	case string:
		pushNullableString(L, &v)
	case *string:
		pushNullableString(L, v)
	case int:
		pushNullableInt(L, &v)
	case *int:
		pushNullableInt(L, v)
	case float64:
		pushNullableNumber(L, &v)
	case *float64:
		pushNullableNumber(L, v)
	case bool:
		pushNullableBoolean(L, &v)
	case *bool:
		pushNullableBoolean(L, v)
	case time.Duration:
		var i int = int(v)
		pushNullableInt(L, &i)
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
		} else if t.Kind() == reflect.Struct {
			L.NewTable()
			val := reflect.ValueOf(v)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				fieldValue := val.Field(i)
				if fieldValue.CanInterface() {
					fieldName := strings.ToLower(field.Name[:1]) + field.Name[1:]
					pushGoType(L, fieldValue.Interface())
					L.SetField(-2, fieldName)
				}
			}
		} else {
			L.PushNil() // Unsupported type
		}
	}
}

func (authorizer *LuaAuthorizer) AuthorizeRequest(ctx context.Context, request *authorization.Request) (bool, error) {
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
	err := lua.DoString(L, authorizer.code)
	if err != nil {
		slog.Error("Error while executing Lua code", "error", err)
		return false, err
	}
	L.Global(authorizationFunctionName)
	if !L.IsFunction(-1) {
		slog.Error("Authorization function not found in Lua code", "functionName", authorizationFunctionName)
		return false, errAuthorizationFunctionNotFound
	}
	pushRequest(L, request)
	err = L.ProtectedCall(1, 1, 0)
	if err != nil {
		slog.Error("Error while calling authorization function", "error", err)
		return false, err
	}
	res := L.ToBoolean(1)
	L.Pop(1)
	slog.Debug("Authorization result", "operation", request.Operation, "isAuthorized", res)
	return res, nil
}

func isReadOnly(operation string) bool {
	var isReadOnly bool
	switch operation {
	case authorization.OperationListBuckets, authorization.OperationHeadBucket, authorization.OperationHeadObject, authorization.OperationListMultipartUploads, authorization.OperationListObjects, authorization.OperationListParts, authorization.OperationGetObject:
		isReadOnly = true
	case authorization.OperationCreateBucket, authorization.OperationDeleteBucket, authorization.OperationCreateMultipartUpload, authorization.OperationCompleteMultipartUpload, authorization.OperationUploadPart, authorization.OperationPutObject, authorization.OperationAbortMultipartUpload, authorization.OperationDeleteObject:
		isReadOnly = false
	}
	return isReadOnly
}

func pushRequest(L *lua.State, request *authorization.Request) {
	pushGoType(L, request)
	L.PushGoFunction(func(L *lua.State) int {
		L.Field(1, "operation")
		operation, _ := L.ToString(-1)
		isReadOnly := isReadOnly(operation)
		L.PushBoolean(isReadOnly)
		return 1
	})
	L.SetField(-2, "isReadOnly")
}
