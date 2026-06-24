package luahelper

import (
	"reflect"
	"strings"
	"time"

	"github.com/Shopify/go-lua"
)

type PushOverride func(L *lua.State, value interface{}) bool

func LowerCamel(name string) string {
	return strings.ToLower(name[:1]) + name[1:]
}

func PushGoValue(L *lua.State, value interface{}) {
	PushGoValueWith(L, value, nil)
}

func PushGoValueWith(L *lua.State, value interface{}, override PushOverride) {
	if isNilValue(reflect.ValueOf(value)) {
		L.PushNil()
		return
	}
	if override != nil && override(L, value) {
		return
	}
	switch v := value.(type) {
	case string:
		L.PushString(v)
	case *string:
		pushNullableString(L, v)
	case int:
		L.PushInteger(v)
	case *int:
		pushNullableInt(L, v)
	case int32:
		L.PushInteger(int(v))
	case int64:
		L.PushNumber(float64(v))
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
	case time.Time:
		L.PushString(v.Format(time.RFC3339Nano))
	default:
		pushReflectValue(L, reflect.ValueOf(value), override)
	}
}

func isNilValue(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func pushNullableString(L *lua.State, value *string) {
	if value == nil {
		L.PushNil()
		return
	}
	L.PushString(*value)
}

func pushNullableInt(L *lua.State, value *int) {
	if value == nil {
		L.PushNil()
		return
	}
	L.PushInteger(*value)
}

func pushNullableNumber(L *lua.State, value *float64) {
	if value == nil {
		L.PushNil()
		return
	}
	L.PushNumber(*value)
}

func pushNullableBoolean(L *lua.State, value *bool) {
	if value == nil {
		L.PushNil()
		return
	}
	L.PushBoolean(*value)
}

func pushReflectValue(L *lua.State, value reflect.Value, override PushOverride) {
	if !value.IsValid() {
		L.PushNil()
		return
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			L.PushNil()
			return
		}
		PushGoValueWith(L, value.Elem().Interface(), override)
		return
	}
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		L.NewTable()
		for i := 0; i < value.Len(); i++ {
			item := value.Index(i)
			if item.CanInterface() {
				PushGoValueWith(L, item.Interface(), override)
				L.RawSetInt(-2, i+1)
			}
		}
		L.PushInteger(value.Len())
		L.RawSetInt(-2, 0)
	case reflect.Map:
		L.NewTable()
		for _, key := range value.MapKeys() {
			mapValue := value.MapIndex(key)
			if key.CanInterface() && mapValue.CanInterface() {
				PushGoValueWith(L, key.Interface(), override)
				PushGoValueWith(L, mapValue.Interface(), override)
				L.SetTable(-3)
			}
		}
	case reflect.Struct:
		L.NewTable()
		t := value.Type()
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			fieldValue := value.Field(i)
			if !field.IsExported() || !fieldValue.CanInterface() {
				continue
			}
			PushGoValueWith(L, fieldValue.Interface(), override)
			L.SetField(-2, LowerCamel(field.Name))
		}
	default:
		L.PushNil()
	}
}
