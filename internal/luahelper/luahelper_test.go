package luahelper

import (
	"testing"

	"github.com/Shopify/go-lua"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nestedTestStruct struct {
	Value string
}

type projectionTestStruct struct {
	Name    string
	Count   int
	Enabled bool
	Nested  nestedTestStruct
	hidden  string
}

type overrideTestValue struct {
	Value string
}

type overrideTestStruct struct {
	Items []overrideTestValue
}

func newTestState() *lua.State {
	L := lua.NewState()
	lua.Require(L, "_G", lua.BaseOpen, true)
	L.Pop(1)
	lua.Require(L, "table", lua.TableOpen, true)
	L.Pop(1)
	return L
}

func TestLowerCamel(t *testing.T) {
	assert.Equal(t, "bucketName", LowerCamel("BucketName"))
	assert.Equal(t, "x", LowerCamel("X"))
}

func TestPushGoValueScalarsAndPointers(t *testing.T) {
	L := newTestState()
	str := "value"
	num := 42
	enabled := true

	PushGoValue(L, str)
	value, ok := L.ToString(-1)
	require.True(t, ok)
	assert.Equal(t, "value", value)
	L.Pop(1)

	PushGoValue(L, &str)
	value, ok = L.ToString(-1)
	require.True(t, ok)
	assert.Equal(t, "value", value)
	L.Pop(1)

	PushGoValue(L, &num)
	intValue, ok := L.ToInteger(-1)
	require.True(t, ok)
	assert.Equal(t, 42, intValue)
	L.Pop(1)

	PushGoValue(L, &enabled)
	assert.True(t, L.ToBoolean(-1))
	L.Pop(1)

	var nilString *string
	PushGoValue(L, nilString)
	assert.True(t, L.IsNil(-1))
}

func TestPushGoValueTypedNils(t *testing.T) {
	L := newTestState()

	var nilString *string
	var nilInterface interface{} = nilString
	PushGoValue(L, nilInterface)
	assert.True(t, L.IsNil(-1))
	L.Pop(1)

	var nilMap map[string]string
	PushGoValue(L, nilMap)
	assert.True(t, L.IsNil(-1))
	L.Pop(1)

	var nilSlice []string
	PushGoValue(L, nilSlice)
	assert.True(t, L.IsNil(-1))
	L.Pop(1)

	var nilFunc func()
	PushGoValue(L, nilFunc)
	assert.True(t, L.IsNil(-1))
}

func TestPushGoValueSliceIncludesLuaArrayEntriesAndZeroLength(t *testing.T) {
	L := newTestState()
	PushGoValue(L, []string{"a", "b"})

	require.True(t, L.IsTable(-1))
	L.RawGetInt(-1, 1)
	value, ok := L.ToString(-1)
	require.True(t, ok)
	assert.Equal(t, "a", value)
	L.Pop(1)

	L.RawGetInt(-1, 2)
	value, ok = L.ToString(-1)
	require.True(t, ok)
	assert.Equal(t, "b", value)
	L.Pop(1)

	L.RawGetInt(-1, 0)
	length, ok := L.ToInteger(-1)
	require.True(t, ok)
	assert.Equal(t, 2, length)
}

func TestPushGoValueMap(t *testing.T) {
	L := newTestState()
	PushGoValue(L, map[string]int{"answer": 42})

	require.True(t, L.IsTable(-1))
	L.Field(-1, "answer")
	value, ok := L.ToInteger(-1)
	require.True(t, ok)
	assert.Equal(t, 42, value)
}

func TestPushGoValueStructUsesLowerCamelExportedFields(t *testing.T) {
	L := newTestState()
	PushGoValue(L, projectionTestStruct{
		Name:    "test",
		Count:   3,
		Enabled: true,
		Nested:  nestedTestStruct{Value: "nested"},
		hidden:  "hidden",
	})

	require.True(t, L.IsTable(-1))
	L.Field(-1, "name")
	name, ok := L.ToString(-1)
	require.True(t, ok)
	assert.Equal(t, "test", name)
	L.Pop(1)

	L.Field(-1, "count")
	count, ok := L.ToInteger(-1)
	require.True(t, ok)
	assert.Equal(t, 3, count)
	L.Pop(1)

	L.Field(-1, "enabled")
	assert.True(t, L.ToBoolean(-1))
	L.Pop(1)

	L.Field(-1, "nested")
	require.True(t, L.IsTable(-1))
	L.Field(-1, "value")
	nestedValue, ok := L.ToString(-1)
	require.True(t, ok)
	assert.Equal(t, "nested", nestedValue)
	L.Pop(2)

	L.Field(-1, "hidden")
	assert.True(t, L.IsNil(-1))
}

func TestPushGoValueWithOverrideRecursesThroughStructsAndSlices(t *testing.T) {
	L := newTestState()
	PushGoValueWith(L, overrideTestStruct{
		Items: []overrideTestValue{{Value: "a"}, {Value: "b"}},
	}, func(L *lua.State, value interface{}) bool {
		item, ok := value.(overrideTestValue)
		if !ok {
			return false
		}
		L.PushString("override-" + item.Value)
		return true
	})

	require.True(t, L.IsTable(-1))
	L.Field(-1, "items")
	require.True(t, L.IsTable(-1))
	L.RawGetInt(-1, 1)
	first, ok := L.ToString(-1)
	require.True(t, ok)
	assert.Equal(t, "override-a", first)
	L.Pop(1)

	L.RawGetInt(-1, 2)
	second, ok := L.ToString(-1)
	require.True(t, ok)
	assert.Equal(t, "override-b", second)
}
