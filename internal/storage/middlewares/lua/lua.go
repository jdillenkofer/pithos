package lua

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	golua "github.com/Shopify/go-lua"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type luaStorageMiddleware struct {
	delegator.DelegatingStorage
	code   string
	tracer trace.Tracer
}

var _ storage.Storage = (*luaStorageMiddleware)(nil)
var _ storage.TransactionalStorage = (*luaStorageMiddleware)(nil)

var (
	contextType      = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType        = reflect.TypeOf((*error)(nil)).Elem()
	readerType       = reflect.TypeOf((*io.Reader)(nil)).Elem()
	readCloserType   = reflect.TypeOf((*io.ReadCloser)(nil)).Elem()
	timeType         = reflect.TypeOf(time.Time{})
	bucketNameType   = reflect.TypeOf(storage.BucketName{})
	objectKeyType    = reflect.TypeOf(storage.ObjectKey{})
	uploadIDType     = reflect.TypeOf(storage.UploadId{})
	readCloserSlice  = reflect.TypeOf([]io.ReadCloser{})
	luaNilReturnType = reflect.TypeOf((*interface{})(nil)).Elem()
)

var luaRegistryCounter atomic.Uint64

var storageErrorByName = map[string]error{
	"NoSuchBucket":               storage.ErrNoSuchBucket,
	"BucketAlreadyExists":        storage.ErrBucketAlreadyExists,
	"BucketNotEmpty":             storage.ErrBucketNotEmpty,
	"NoSuchKey":                  storage.ErrNoSuchKey,
	"BadDigest":                  storage.ErrBadDigest,
	"not implemented":            storage.ErrNotImplemented,
	"NotImplemented":             storage.ErrNotImplemented,
	"EntityTooLarge":             storage.ErrEntityTooLarge,
	"PreconditionFailed":         storage.ErrPreconditionFailed,
	"NotModified":                storage.ErrNotModified,
	"invalid bucket name":        storage.ErrInvalidBucketName,
	"InvalidBucketName":          storage.ErrInvalidBucketName,
	"invalid object key":         storage.ErrInvalidObjectKey,
	"InvalidObjectKey":           storage.ErrInvalidObjectKey,
	"invalid upload ID":          storage.ErrInvalidUploadId,
	"InvalidUploadId":            storage.ErrInvalidUploadId,
	"InvalidRange":               storage.ErrInvalidRange,
	"NoSuchWebsiteConfiguration": storage.ErrNoSuchWebsiteConfiguration,
	"TooManyParts":               storage.ErrTooManyParts,
	"InvalidWriteOffset":         storage.ErrInvalidWriteOffset,
	"CASFailure":                 storage.ErrCASFailure,
}

func NewStorageMiddleware(innerStorage storage.Storage, code string) (storage.Storage, error) {
	L := newLuaState()
	if err := golua.DoString(L, code); err != nil {
		return nil, err
	}
	return &luaStorageMiddleware{
		DelegatingStorage: delegator.Wrap(innerStorage),
		code:              code,
		tracer:            otel.Tracer("internal/storage/middlewares/lua"),
	}, nil
}

func newLuaState() *golua.State {
	L := golua.NewState()
	golua.Require(L, "_G", golua.BaseOpen, true)
	L.Pop(1)
	golua.Require(L, "table", golua.TableOpen, true)
	L.Pop(1)
	golua.Require(L, "string", golua.StringOpen, true)
	L.Pop(1)
	golua.Require(L, "math", golua.MathOpen, true)
	L.Pop(1)
	return L
}

func (m *luaStorageMiddleware) call(ctx context.Context, methodName string, args []interface{}, fallback func() []interface{}) ([]interface{}, error) {
	_, span := m.tracer.Start(ctx, "LuaStorageMiddleware."+methodName)
	defer span.End()

	L := newLuaState()
	m.pushInnerStorage(L)
	L.SetGlobal("innerStorage")
	if err := golua.DoString(L, m.code); err != nil {
		return nil, err
	}

	L.Global(methodName)
	if !L.IsFunction(-1) {
		L.Pop(1)
		return fallback(), nil
	}
	for _, arg := range args {
		if err := pushLuaValue(L, arg); err != nil {
			return nil, err
		}
	}

	method := reflect.ValueOf(m.Next).MethodByName(methodName)
	if !method.IsValid() {
		return nil, fmt.Errorf("storage method %s not found", methodName)
	}
	returnCount := method.Type().NumOut()
	if err := L.ProtectedCall(len(args), returnCount, 0); err != nil {
		return nil, err
	}

	results := make([]interface{}, returnCount)
	for i := 0; i < returnCount; i++ {
		value, err := luaValueToGo(L, i+1, method.Type().Out(i))
		if err != nil {
			return nil, err
		}
		results[i] = value
	}
	L.Pop(returnCount)
	return results, nil
}

func (m *luaStorageMiddleware) pushInnerStorage(L *golua.State) {
	L.NewTable()
	next := reflect.ValueOf(m.Next)
	nextType := next.Type()
	for i := 0; i < nextType.NumMethod(); i++ {
		method := nextType.Method(i)
		methodName := method.Name
		L.PushGoFunction(func(L *golua.State) int {
			methodValue := reflect.ValueOf(m.Next).MethodByName(methodName)
			methodType := methodValue.Type()
			args := make([]reflect.Value, methodType.NumIn())
			for argIndex := 0; argIndex < methodType.NumIn(); argIndex++ {
				arg, err := luaValueToGo(L, argIndex+1, methodType.In(argIndex))
				if err != nil {
					L.PushString(err.Error())
					L.Error()
					return 0
				}
				args[argIndex] = reflect.ValueOf(arg)
			}
			results := methodValue.Call(args)
			for _, result := range results {
				if result.Type().Implements(errorType) {
					if result.IsNil() {
						L.PushNil()
					} else {
						L.PushString(result.Interface().(error).Error())
					}
					continue
				}
				if err := pushLuaValue(L, result.Interface()); err != nil {
					L.PushString(err.Error())
					L.Error()
					return 0
				}
			}
			return len(results)
		})
		L.SetField(-2, methodName)
	}
}

func pushLuaValue(L *golua.State, value interface{}) error {
	if value == nil {
		L.PushNil()
		return nil
	}
	if err, ok := value.(error); ok {
		if err == nil {
			L.PushNil()
		} else {
			L.PushString(err.Error())
		}
		return nil
	}
	switch v := value.(type) {
	case context.Context:
		L.PushUserData(v)
	case storage.BucketName:
		L.PushString(v.String())
	case storage.ObjectKey:
		L.PushString(v.String())
	case storage.UploadId:
		L.PushString(v.String())
	case io.ReadCloser:
		pushReaderTable(L, v, v)
	case io.Reader:
		pushReaderTable(L, v, nil)
	case time.Time:
		L.PushString(v.Format(time.RFC3339Nano))
	case string:
		L.PushString(v)
	case bool:
		L.PushBoolean(v)
	case int:
		L.PushInteger(v)
	case int32:
		L.PushInteger(int(v))
	case int64:
		L.PushNumber(float64(v))
	default:
		return pushReflectValue(L, reflect.ValueOf(value))
	}
	return nil
}

func pushReflectValue(L *golua.State, value reflect.Value) error {
	if !value.IsValid() {
		L.PushNil()
		return nil
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			L.PushNil()
			return nil
		}
		return pushLuaValue(L, value.Elem().Interface())
	}
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		if value.Type() == readCloserSlice {
			L.NewTable()
			for i := 0; i < value.Len(); i++ {
				if err := pushLuaValue(L, value.Index(i).Interface()); err != nil {
					return err
				}
				L.RawSetInt(-2, i+1)
			}
			return nil
		}
		L.NewTable()
		for i := 0; i < value.Len(); i++ {
			if err := pushLuaValue(L, value.Index(i).Interface()); err != nil {
				return err
			}
			L.RawSetInt(-2, i+1)
		}
	case reflect.Struct:
		if value.Type() == timeType {
			L.PushString(value.Interface().(time.Time).Format(time.RFC3339Nano))
			return nil
		}
		L.NewTable()
		t := value.Type()
		for i := 0; i < value.NumField(); i++ {
			field := t.Field(i)
			fieldValue := value.Field(i)
			if !field.IsExported() || !fieldValue.CanInterface() {
				continue
			}
			if err := pushLuaValue(L, fieldValue.Interface()); err != nil {
				return err
			}
			L.SetField(-2, lowerCamel(field.Name))
		}
	case reflect.String:
		L.PushString(value.String())
	case reflect.Bool:
		L.PushBoolean(value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		L.PushInteger(int(value.Int()))
	case reflect.Int64:
		L.PushNumber(float64(value.Int()))
	default:
		L.PushUserData(value.Interface())
	}
	return nil
}

func luaValueToGo(L *golua.State, index int, targetType reflect.Type) (interface{}, error) {
	index = L.AbsIndex(index)
	if targetType == luaNilReturnType {
		return L.ToValue(index), nil
	}
	if targetType.Implements(errorType) {
		return luaValueToError(L, index), nil
	}
	if L.IsNil(index) {
		return reflect.Zero(targetType).Interface(), nil
	}
	if targetType == contextType {
		ctx, ok := L.ToUserData(index).(context.Context)
		if !ok {
			return nil, fmt.Errorf("context.Context expected")
		}
		return ctx, nil
	}
	if targetType == readerType {
		if reader, ok := L.ToUserData(index).(io.Reader); ok {
			return reader, nil
		}
		if L.IsTable(index) {
			return newLuaReader(L, index), nil
		}
		s, ok := L.ToString(index)
		if !ok {
			return nil, fmt.Errorf("reader string expected")
		}
		return strings.NewReader(s), nil
	}
	if targetType == readCloserType {
		if reader, ok := L.ToUserData(index).(io.ReadCloser); ok {
			return reader, nil
		}
		if L.IsTable(index) {
			return newLuaReader(L, index), nil
		}
		s, ok := L.ToString(index)
		if !ok {
			return nil, fmt.Errorf("read closer string expected")
		}
		return ioutils.NewByteReadSeekCloser([]byte(s)), nil
	}
	if targetType == bucketNameType {
		s, ok := L.ToString(index)
		if !ok {
			return nil, fmt.Errorf("bucket name string expected")
		}
		return storage.NewBucketName(s)
	}
	if targetType == objectKeyType {
		s, ok := L.ToString(index)
		if !ok {
			return nil, fmt.Errorf("object key string expected")
		}
		return storage.NewObjectKey(s)
	}
	if targetType == uploadIDType {
		s, ok := L.ToString(index)
		if !ok {
			return nil, fmt.Errorf("upload ID string expected")
		}
		return storage.NewUploadId(s)
	}
	if targetType == timeType {
		s, ok := L.ToString(index)
		if !ok {
			return nil, fmt.Errorf("time string expected")
		}
		return time.Parse(time.RFC3339Nano, s)
	}
	if targetType.Kind() == reflect.Ptr {
		if L.IsNil(index) {
			return reflect.Zero(targetType).Interface(), nil
		}
		value, err := luaValueToGo(L, index, targetType.Elem())
		if err != nil {
			return nil, err
		}
		ptr := reflect.New(targetType.Elem())
		ptr.Elem().Set(reflect.ValueOf(value))
		return ptr.Interface(), nil
	}
	if targetType.Kind() == reflect.Slice {
		if targetType == readCloserSlice {
			return luaTableToReadClosers(L, index)
		}
		if !L.IsTable(index) {
			return nil, fmt.Errorf("table expected for %s", targetType.String())
		}
		length := L.RawLength(index)
		result := reflect.MakeSlice(targetType, 0, length)
		for i := 1; i <= length; i++ {
			L.RawGetInt(index, i)
			item, err := luaValueToGo(L, -1, targetType.Elem())
			L.Pop(1)
			if err != nil {
				return nil, err
			}
			result = reflect.Append(result, reflect.ValueOf(item))
		}
		return result.Interface(), nil
	}
	if targetType.Kind() == reflect.Struct {
		return luaTableToStruct(L, index, targetType)
	}
	switch targetType.Kind() {
	case reflect.String:
		s, ok := L.ToString(index)
		if !ok {
			return nil, fmt.Errorf("string expected")
		}
		return s, nil
	case reflect.Bool:
		return L.ToBoolean(index), nil
	case reflect.Int:
		i, ok := L.ToInteger(index)
		if !ok {
			return nil, fmt.Errorf("integer expected")
		}
		return i, nil
	case reflect.Int32:
		i, ok := L.ToInteger(index)
		if !ok {
			return nil, fmt.Errorf("int32 expected")
		}
		return int32(i), nil
	case reflect.Int64:
		i, ok := L.ToInteger(index)
		if !ok {
			return nil, fmt.Errorf("int64 expected")
		}
		return int64(i), nil
	case reflect.Interface:
		value := L.ToValue(index)
		if value == nil {
			return reflect.Zero(targetType).Interface(), nil
		}
		if reflect.TypeOf(value).AssignableTo(targetType) {
			return value, nil
		}
	}
	userData := L.ToUserData(index)
	if userData != nil && reflect.TypeOf(userData).AssignableTo(targetType) {
		return userData, nil
	}
	return nil, fmt.Errorf("unsupported Lua conversion to %s", targetType.String())
}

func luaTableToStruct(L *golua.State, index int, targetType reflect.Type) (interface{}, error) {
	if !L.IsTable(index) {
		userData := L.ToUserData(index)
		if userData != nil && reflect.TypeOf(userData).AssignableTo(targetType) {
			return userData, nil
		}
		return nil, fmt.Errorf("table expected for %s", targetType.String())
	}
	index = L.AbsIndex(index)
	result := reflect.New(targetType).Elem()
	for i := 0; i < targetType.NumField(); i++ {
		field := targetType.Field(i)
		if !field.IsExported() {
			continue
		}
		L.Field(index, lowerCamel(field.Name))
		if L.IsNil(-1) {
			L.Pop(1)
			continue
		}
		value, err := luaValueToGo(L, -1, field.Type)
		L.Pop(1)
		if err != nil {
			return nil, err
		}
		result.Field(i).Set(reflect.ValueOf(value))
	}
	return result.Interface(), nil
}

func luaTableToReadClosers(L *golua.State, index int) ([]io.ReadCloser, error) {
	if !L.IsTable(index) {
		return nil, fmt.Errorf("table expected for readers")
	}
	index = L.AbsIndex(index)
	length := L.RawLength(index)
	readers := make([]io.ReadCloser, 0, length)
	for i := 1; i <= length; i++ {
		L.RawGetInt(index, i)
		if rc, ok := L.ToUserData(-1).(io.ReadCloser); ok {
			readers = append(readers, rc)
			L.Pop(1)
			continue
		}
		if L.IsTable(-1) {
			readers = append(readers, newLuaReader(L, -1))
			L.Pop(1)
			continue
		}
		s, ok := L.ToString(-1)
		L.Pop(1)
		if !ok {
			return nil, fmt.Errorf("reader string expected")
		}
		readers = append(readers, ioutils.NewByteReadSeekCloser([]byte(s)))
	}
	return readers, nil
}

func luaValueToError(L *golua.State, index int) error {
	if L.IsNil(index) {
		return nil
	}
	if err, ok := L.ToUserData(index).(error); ok {
		return err
	}
	s, ok := L.ToString(index)
	if !ok || s == "" {
		return nil
	}
	if err, ok := storageErrorByName[s]; ok {
		return err
	}
	return errors.New(s)
}

func lowerCamel(name string) string {
	return strings.ToLower(name[:1]) + name[1:]
}

func pushReaderTable(L *golua.State, reader io.Reader, closer io.Closer) {
	L.NewTable()
	L.PushUserData(reader)
	L.SetField(-2, "__reader")
	if closer != nil {
		L.PushUserData(closer)
		L.SetField(-2, "__closer")
	}
	L.PushGoFunction(func(L *golua.State) int {
		L.Field(1, "__reader")
		reader, ok := L.ToUserData(-1).(io.Reader)
		L.Pop(1)
		if !ok {
			L.PushNil()
			L.PushString("reader missing")
			return 2
		}
		size, ok := L.ToInteger(2)
		if !ok || size <= 0 {
			size = 32 * 1024
		}
		buf := make([]byte, size)
		n, err := reader.Read(buf)
		if n > 0 {
			L.PushString(string(buf[:n]))
			if err != nil && err != io.EOF {
				L.PushString(err.Error())
			} else {
				L.PushNil()
			}
			return 2
		}
		L.PushNil()
		if err != nil && err != io.EOF {
			L.PushString(err.Error())
		} else {
			L.PushNil()
		}
		return 2
	})
	L.SetField(-2, "read")
	L.PushGoFunction(func(L *golua.State) int {
		L.Field(1, "__closer")
		closer, ok := L.ToUserData(-1).(io.Closer)
		L.Pop(1)
		if !ok {
			L.PushNil()
			return 1
		}
		if err := closer.Close(); err != nil {
			L.PushString(err.Error())
			return 1
		}
		L.PushNil()
		return 1
	})
	L.SetField(-2, "close")
}

type luaReadCloser struct {
	L      *golua.State
	key    string
	buffer []byte
	eof    bool
	mu     sync.Mutex
}

func newLuaReader(L *golua.State, index int) *luaReadCloser {
	key := fmt.Sprintf("pithos_lua_reader_%d", luaRegistryCounter.Add(1))
	L.PushValue(index)
	L.SetField(golua.RegistryIndex, key)
	return &luaReadCloser{L: L, key: key}
}

func (r *luaReadCloser) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buffer) == 0 && !r.eof {
		if err := r.readNextChunk(len(p)); err != nil {
			return 0, err
		}
	}
	if len(r.buffer) == 0 && r.eof {
		return 0, io.EOF
	}
	n := copy(p, r.buffer)
	r.buffer = r.buffer[n:]
	return n, nil
}

func (r *luaReadCloser) readNextChunk(size int) error {
	if size <= 0 {
		size = 32 * 1024
	}
	r.L.Field(golua.RegistryIndex, r.key)
	tableIndex := r.L.AbsIndex(-1)
	r.L.Field(tableIndex, "read")
	if !r.L.IsFunction(-1) {
		r.L.Pop(2)
		return errors.New("Lua reader does not provide read")
	}
	r.L.Insert(-2)
	r.L.PushInteger(size)
	if err := r.L.ProtectedCall(2, 2, 0); err != nil {
		return err
	}
	if !r.L.IsNil(-1) {
		errText, _ := r.L.ToString(-1)
		r.L.Pop(2)
		if errText == "" {
			errText = "Lua reader error"
		}
		return errors.New(errText)
	}
	if r.L.IsNil(-2) {
		r.eof = true
		r.L.Pop(2)
		return nil
	}
	chunk, ok := r.L.ToString(-2)
	r.L.Pop(2)
	if !ok {
		return errors.New("Lua reader read must return string or nil")
	}
	if chunk == "" {
		r.eof = true
		return nil
	}
	r.buffer = []byte(chunk)
	return nil
}

func (r *luaReadCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.L.Field(golua.RegistryIndex, r.key)
	tableIndex := r.L.AbsIndex(-1)
	r.L.Field(tableIndex, "close")
	if !r.L.IsFunction(-1) {
		r.L.Pop(2)
		return nil
	}
	r.L.Insert(-2)
	if err := r.L.ProtectedCall(1, 1, 0); err != nil {
		return err
	}
	defer r.L.Pop(1)
	if r.L.IsNil(-1) {
		return nil
	}
	errText, _ := r.L.ToString(-1)
	if errText == "" {
		return errors.New("Lua reader close error")
	}
	return errors.New(errText)
}

func oneResult(results []interface{}, err error) error {
	if err != nil {
		return err
	}
	if len(results) == 0 || results[0] == nil {
		return nil
	}
	return results[0].(error)
}

func resultError(value interface{}) error {
	if value == nil {
		return nil
	}
	return value.(error)
}

func (m *luaStorageMiddleware) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return delegator.WithTransaction(ctx, opts, m.Next, m, fn)
}

func (m *luaStorageMiddleware) Start(ctx context.Context) error {
	return oneResult(m.call(ctx, "Start", []interface{}{ctx}, func() []interface{} {
		return []interface{}{m.Next.Start(ctx)}
	}))
}

func (m *luaStorageMiddleware) Stop(ctx context.Context) error {
	return oneResult(m.call(ctx, "Stop", []interface{}{ctx}, func() []interface{} {
		return []interface{}{m.Next.Stop(ctx)}
	}))
}

func (m *luaStorageMiddleware) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	return oneResult(m.call(ctx, "CreateBucket", []interface{}{ctx, bucketName}, func() []interface{} {
		return []interface{}{m.Next.CreateBucket(ctx, bucketName)}
	}))
}

func (m *luaStorageMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	return oneResult(m.call(ctx, "DeleteBucket", []interface{}{ctx, bucketName}, func() []interface{} {
		return []interface{}{m.Next.DeleteBucket(ctx, bucketName)}
	}))
}

func (m *luaStorageMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	results, err := m.call(ctx, "ListBuckets", []interface{}{ctx}, func() []interface{} {
		buckets, err := m.Next.ListBuckets(ctx)
		return []interface{}{buckets, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].([]storage.Bucket), resultError(results[1])
}

func (m *luaStorageMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	results, err := m.call(ctx, "HeadBucket", []interface{}{ctx, bucketName}, func() []interface{} {
		bucket, err := m.Next.HeadBucket(ctx, bucketName)
		return []interface{}{bucket, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.Bucket), resultError(results[1])
}

func (m *luaStorageMiddleware) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	results, err := m.call(ctx, "GetBucketWebsiteConfiguration", []interface{}{ctx, bucketName}, func() []interface{} {
		config, err := m.Next.GetBucketWebsiteConfiguration(ctx, bucketName)
		return []interface{}{config, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.WebsiteConfiguration), resultError(results[1])
}

func (m *luaStorageMiddleware) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	return oneResult(m.call(ctx, "PutBucketWebsiteConfiguration", []interface{}{ctx, bucketName, config}, func() []interface{} {
		return []interface{}{m.Next.PutBucketWebsiteConfiguration(ctx, bucketName, config)}
	}))
}

func (m *luaStorageMiddleware) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return oneResult(m.call(ctx, "DeleteBucketWebsiteConfiguration", []interface{}{ctx, bucketName}, func() []interface{} {
		return []interface{}{m.Next.DeleteBucketWebsiteConfiguration(ctx, bucketName)}
	}))
}

func (m *luaStorageMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	results, err := m.call(ctx, "ListObjects", []interface{}{ctx, bucketName, opts}, func() []interface{} {
		result, err := m.Next.ListObjects(ctx, bucketName, opts)
		return []interface{}{result, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.ListBucketResult), resultError(results[1])
}

func (m *luaStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	results, err := m.call(ctx, "HeadObject", []interface{}{ctx, bucketName, key, opts}, func() []interface{} {
		object, err := m.Next.HeadObject(ctx, bucketName, key, opts)
		return []interface{}{object, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.Object), resultError(results[1])
}

func (m *luaStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	results, err := m.call(ctx, "GetObject", []interface{}{ctx, bucketName, key, ranges, opts}, func() []interface{} {
		object, readers, err := m.Next.GetObject(ctx, bucketName, key, ranges, opts)
		return []interface{}{object, readers, err}
	})
	if err != nil {
		return nil, nil, err
	}
	return results[0].(*storage.Object), results[1].([]io.ReadCloser), resultError(results[2])
}

func (m *luaStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	results, err := m.call(ctx, "PutObject", []interface{}{ctx, bucketName, key, contentType, data, checksumInput, opts}, func() []interface{} {
		result, err := m.Next.PutObject(ctx, bucketName, key, contentType, data, checksumInput, opts)
		return []interface{}{result, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.PutObjectResult), resultError(results[1])
}

func (m *luaStorageMiddleware) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	results, err := m.call(ctx, "AppendObject", []interface{}{ctx, bucketName, key, data, checksumInput, opts}, func() []interface{} {
		result, err := m.Next.AppendObject(ctx, bucketName, key, data, checksumInput, opts)
		return []interface{}{result, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.AppendObjectResult), resultError(results[1])
}

func (m *luaStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	return oneResult(m.call(ctx, "DeleteObject", []interface{}{ctx, bucketName, key, opts}, func() []interface{} {
		return []interface{}{m.Next.DeleteObject(ctx, bucketName, key, opts)}
	}))
}

func (m *luaStorageMiddleware) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	results, err := m.call(ctx, "DeleteObjects", []interface{}{ctx, bucketName, entries}, func() []interface{} {
		result, err := m.Next.DeleteObjects(ctx, bucketName, entries)
		return []interface{}{result, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.DeleteObjectsResult), resultError(results[1])
}

func (m *luaStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	results, err := m.call(ctx, "CreateMultipartUpload", []interface{}{ctx, bucketName, key, contentType, checksumType}, func() []interface{} {
		result, err := m.Next.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
		return []interface{}{result, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.InitiateMultipartUploadResult), resultError(results[1])
}

func (m *luaStorageMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	results, err := m.call(ctx, "UploadPart", []interface{}{ctx, bucketName, key, uploadId, partNumber, data, checksumInput}, func() []interface{} {
		result, err := m.Next.UploadPart(ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
		return []interface{}{result, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.UploadPartResult), resultError(results[1])
}

func (m *luaStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	results, err := m.call(ctx, "CompleteMultipartUpload", []interface{}{ctx, bucketName, key, uploadId, checksumInput, opts}, func() []interface{} {
		result, err := m.Next.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, opts)
		return []interface{}{result, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.CompleteMultipartUploadResult), resultError(results[1])
}

func (m *luaStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	return oneResult(m.call(ctx, "AbortMultipartUpload", []interface{}{ctx, bucketName, key, uploadId}, func() []interface{} {
		return []interface{}{m.Next.AbortMultipartUpload(ctx, bucketName, key, uploadId)}
	}))
}

func (m *luaStorageMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	results, err := m.call(ctx, "ListMultipartUploads", []interface{}{ctx, bucketName, opts}, func() []interface{} {
		result, err := m.Next.ListMultipartUploads(ctx, bucketName, opts)
		return []interface{}{result, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.ListMultipartUploadsResult), resultError(results[1])
}

func (m *luaStorageMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	results, err := m.call(ctx, "ListParts", []interface{}{ctx, bucketName, key, uploadId, opts}, func() []interface{} {
		result, err := m.Next.ListParts(ctx, bucketName, key, uploadId, opts)
		return []interface{}{result, err}
	})
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.ListPartsResult), resultError(results[1])
}
