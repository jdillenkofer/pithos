package lua

import (
	"reflect"
	"strings"
	"time"

	"github.com/Shopify/go-lua"
	"github.com/jdillenkofer/pithos/internal/authorization"
)

const authorizationFunctionName = "authorizeRequest"

type authorizationError struct {
	error
}

type LuaAuthorizer struct {
	code string
}

func NewLuaAuthorizer(code string) *LuaAuthorizer {
	return &LuaAuthorizer{
		code: code,
	}
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

func (authorizer *LuaAuthorizer) AuthorizeRequest(request *authorization.Request) (bool, error) {
	L := lua.NewState()
	lua.OpenLibraries(L)
	err := lua.DoString(L, authorizer.code)
	if err != nil {
		return false, &authorizationError{error: err}
	}
	L.Global(authorizationFunctionName)
	pushGoType(L, request)
	err = L.ProtectedCall(1, 1, 0)
	if err != nil {
		return false, &authorizationError{error: err}
	}
	res := L.ToBoolean(1)
	L.Pop(1)
	return res, nil
}
