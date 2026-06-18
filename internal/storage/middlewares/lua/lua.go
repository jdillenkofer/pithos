package lua

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	golua "github.com/Shopify/go-lua"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/luahelper"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/webhookoutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/jdillenkofer/pithos/internal/storage/webhook"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type luaStorageMiddleware struct {
	delegator.DelegatingStorage
	code       string
	db         database.Database
	dispatcher *webhook.Dispatcher
	tracer     trace.Tracer
}

var _ storage.Storage = (*luaStorageMiddleware)(nil)
var _ storage.TransactionalStorage = (*luaStorageMiddleware)(nil)

// mutatingMethods are the storage operations the middleware wraps in a
// read-write transaction when webhooks are enabled, so a Lua override and any
// webhooks it enqueues commit (or roll back) atomically with the operation.
// Read/streaming methods (e.g. GetObject) are intentionally excluded: their
// readers are bound to the transaction and would be invalidated by an early
// commit.
var mutatingMethods = map[string]bool{
	"CreateBucket":                     true,
	"DeleteBucket":                     true,
	"PutObject":                        true,
	"AppendObject":                     true,
	"DeleteObject":                     true,
	"DeleteObjects":                    true,
	"CreateMultipartUpload":            true,
	"UploadPart":                       true,
	"CompleteMultipartUpload":          true,
	"AbortMultipartUpload":             true,
	"PutBucketWebsiteConfiguration":    true,
	"DeleteBucketWebsiteConfiguration": true,
}

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
	return NewStorageMiddlewareWithWebhooks(innerStorage, code, nil, nil)
}

// NewStorageMiddlewareWithWebhooks builds the Lua middleware with webhook
// support. When db and dispatcher are non-nil, mutating Lua overrides run inside
// a read-write transaction and the script gains a global `webhooks.enqueue(ctx,
// evt)` that persists a webhook in the same transaction and delivers it after
// commit. db must be the same database.Database instance used by the storage
// below this middleware, so the webhook row and the storage write share one SQL
// transaction.
func NewStorageMiddlewareWithWebhooks(innerStorage storage.Storage, code string, db database.Database, dispatcher *webhook.Dispatcher) (storage.Storage, error) {
	L := newLuaState()
	if err := golua.DoString(L, code); err != nil {
		return nil, err
	}
	return &luaStorageMiddleware{
		DelegatingStorage: delegator.Wrap(innerStorage),
		code:              code,
		db:                db,
		dispatcher:        dispatcher,
		tracer:            otel.Tracer("internal/storage/middlewares/lua"),
	}, nil
}

func (m *luaStorageMiddleware) webhooksEnabled() bool {
	return m.db != nil && m.dispatcher != nil
}

func (m *luaStorageMiddleware) call(ctx context.Context, methodName string, args ...interface{}) ([]interface{}, error) {
	_, span := m.tracer.Start(ctx, "LuaStorageMiddleware."+methodName)
	defer span.End()

	method, err := m.lookupNextMethod(methodName)
	if err != nil {
		return nil, err
	}

	L := newLuaState()
	m.pushInnerStorage(L)
	L.SetGlobal("innerStorage")
	if m.webhooksEnabled() {
		m.pushWebhooks(L)
		L.SetGlobal("webhooks")
	}
	if err := golua.DoString(L, m.code); err != nil {
		return nil, err
	}

	L.Global(methodName)
	if !L.IsFunction(-1) {
		L.Pop(1)
		return m.callNextMethod(method, args)
	}

	if m.webhooksEnabled() && mutatingMethods[methodName] {
		var results []interface{}
		err := database.WithTx(ctx, m.db, &sql.TxOptions{ReadOnly: false}, func(txCtx context.Context, tx database.Tx) error {
			txArgs := make([]interface{}, len(args))
			copy(txArgs, args)
			if len(txArgs) > 0 {
				txArgs[0] = txCtx
			}
			var invokeErr error
			results, invokeErr = m.invokeLuaFunction(L, method, txArgs)
			if invokeErr != nil {
				return invokeErr
			}
			// Propagate a storage error returned by the Lua override so the
			// transaction rolls back (and any enqueued webhook is discarded).
			return firstErrorResult(results)
		})
		if err != nil {
			return nil, err
		}
		return results, nil
	}

	return m.invokeLuaFunction(L, method, args)
}

func (m *luaStorageMiddleware) invokeLuaFunction(L *golua.State, method reflect.Value, args []interface{}) ([]interface{}, error) {
	for _, arg := range args {
		pushLuaValue(L, arg)
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

// firstErrorResult returns the first non-nil error among decoded Lua results.
func firstErrorResult(results []interface{}) error {
	for _, result := range results {
		if err, ok := result.(error); ok && err != nil {
			return err
		}
	}
	return nil
}

func (m *luaStorageMiddleware) lookupNextMethod(methodName string) (reflect.Value, error) {
	method := reflect.ValueOf(m.Next).MethodByName(methodName)
	if !method.IsValid() {
		return reflect.Value{}, fmt.Errorf("storage method %s not found", methodName)
	}
	return method, nil
}

func (m *luaStorageMiddleware) callNextMethod(method reflect.Value, args []interface{}) ([]interface{}, error) {
	methodType := method.Type()
	if len(args) != methodType.NumIn() {
		return nil, fmt.Errorf("expected %d args got %d", methodType.NumIn(), len(args))
	}
	values := make([]reflect.Value, len(args))
	for i, arg := range args {
		values[i] = reflect.ValueOf(arg)
	}
	return reflectResultsToInterfaces(method.Call(values)), nil
}

func reflectResultsToInterfaces(results []reflect.Value) []interface{} {
	interfaces := make([]interface{}, len(results))
	for i, result := range results {
		if result.Type().Implements(errorType) && result.IsNil() {
			interfaces[i] = nil
			continue
		}
		interfaces[i] = result.Interface()
	}
	return interfaces
}

func newLuaState() *golua.State {
	L := golua.NewState()
	golua.OpenLibraries(L)
	return L
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
			for _, result := range reflectResultsToInterfaces(methodValue.Call(args)) {
				pushLuaValue(L, result)
			}
			return methodType.NumOut()
		})
		L.SetField(-2, methodName)
	}
}

func (m *luaStorageMiddleware) pushWebhooks(L *golua.State) {
	L.NewTable()
	L.PushGoFunction(func(L *golua.State) int {
		ctx, ok := L.ToUserData(1).(context.Context)
		if !ok {
			L.PushString("webhooks.enqueue requires a context as the first argument")
			L.Error()
			return 0
		}
		tx, ok := database.TxControllerFromContext(ctx)
		if !ok {
			L.PushString("webhooks.enqueue must be called inside a mutating operation")
			L.Error()
			return 0
		}
		entry, err := readWebhookEntry(L, 2)
		if err != nil {
			L.PushString(err.Error())
			L.Error()
			return 0
		}
		if err := m.dispatcher.Enqueue(ctx, tx, entry); err != nil {
			L.PushString(err.Error())
			L.Error()
			return 0
		}
		return 0
	})
	L.SetField(-2, "enqueue")
}

func readWebhookEntry(L *golua.State, index int) (*webhookoutboxentry.Entity, error) {
	if !L.IsTable(index) {
		return nil, errors.New("webhooks.enqueue requires a table as the second argument")
	}
	index = L.AbsIndex(index)

	url, err := requireStringField(L, index, "url")
	if err != nil {
		return nil, err
	}

	method := optionalStringField(L, index, "method")
	if method == "" {
		method = http.MethodPost
	}

	var body []byte
	L.Field(index, "body")
	if !L.IsNil(-1) {
		s, ok := L.ToString(-1)
		if !ok {
			L.Pop(1)
			return nil, errors.New("webhooks.enqueue body must be a string")
		}
		body = []byte(s)
	}
	L.Pop(1)

	headers, err := readWebhookHeaders(L, index)
	if err != nil {
		return nil, err
	}

	return &webhookoutboxentry.Entity{
		Url:     url,
		Method:  method,
		Headers: headers,
		Body:    body,
	}, nil
}

func readWebhookHeaders(L *golua.State, index int) (*string, error) {
	L.Field(index, "headers")
	defer L.Pop(1)
	if !L.IsTable(-1) {
		return nil, nil
	}
	headersIndex := L.AbsIndex(-1)
	headers := map[string]string{}
	L.PushNil()
	for L.Next(headersIndex) {
		key, keyOk := L.ToString(-2)
		value, valueOk := L.ToString(-1)
		if keyOk && valueOk {
			headers[key] = value
		}
		L.Pop(1) // pop value, keep key for the next iteration
	}
	if len(headers) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(headers)
	if err != nil {
		return nil, err
	}
	s := string(encoded)
	return &s, nil
}

func requireStringField(L *golua.State, index int, name string) (string, error) {
	L.Field(index, name)
	defer L.Pop(1)
	s, ok := L.ToString(-1)
	if !ok || s == "" {
		return "", fmt.Errorf("webhooks.enqueue requires a non-empty string %q field", name)
	}
	return s, nil
}

func optionalStringField(L *golua.State, index int, name string) string {
	L.Field(index, name)
	defer L.Pop(1)
	s, _ := L.ToString(-1)
	return s
}

func pushLuaValue(L *golua.State, value interface{}) {
	luahelper.PushGoValueWith(L, value, pushStorageLuaValue)
}

func pushStorageLuaValue(L *golua.State, value interface{}) bool {
	if err, ok := value.(error); ok {
		if err == nil {
			L.PushNil()
		} else {
			L.PushString(err.Error())
		}
		return true
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
	default:
		return false
	}
	return true
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
		L.Field(index, luahelper.LowerCamel(field.Name))
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
	if m.dispatcher != nil {
		if err := m.dispatcher.Start(ctx); err != nil {
			return err
		}
	}
	return m.Next.Start(ctx)
}

func (m *luaStorageMiddleware) Stop(ctx context.Context) error {
	if m.dispatcher != nil {
		if err := m.dispatcher.Stop(ctx); err != nil {
			return err
		}
	}
	return m.Next.Stop(ctx)
}

func (m *luaStorageMiddleware) CreateBucket(ctx context.Context, bucketName storage.BucketName) error {
	return oneResult(m.call(ctx, "CreateBucket", ctx, bucketName))
}

func (m *luaStorageMiddleware) DeleteBucket(ctx context.Context, bucketName storage.BucketName) error {
	return oneResult(m.call(ctx, "DeleteBucket", ctx, bucketName))
}

func (m *luaStorageMiddleware) ListBuckets(ctx context.Context) ([]storage.Bucket, error) {
	results, err := m.call(ctx, "ListBuckets", ctx)
	if err != nil {
		return nil, err
	}
	return results[0].([]storage.Bucket), resultError(results[1])
}

func (m *luaStorageMiddleware) HeadBucket(ctx context.Context, bucketName storage.BucketName) (*storage.Bucket, error) {
	results, err := m.call(ctx, "HeadBucket", ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.Bucket), resultError(results[1])
}

func (m *luaStorageMiddleware) GetBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.WebsiteConfiguration, error) {
	results, err := m.call(ctx, "GetBucketWebsiteConfiguration", ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.WebsiteConfiguration), resultError(results[1])
}

func (m *luaStorageMiddleware) PutBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.WebsiteConfiguration) error {
	return oneResult(m.call(ctx, "PutBucketWebsiteConfiguration", ctx, bucketName, config))
}

func (m *luaStorageMiddleware) DeleteBucketWebsiteConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	return oneResult(m.call(ctx, "DeleteBucketWebsiteConfiguration", ctx, bucketName))
}

func (m *luaStorageMiddleware) ListObjects(ctx context.Context, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	results, err := m.call(ctx, "ListObjects", ctx, bucketName, opts)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.ListBucketResult), resultError(results[1])
}

func (m *luaStorageMiddleware) HeadObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	results, err := m.call(ctx, "HeadObject", ctx, bucketName, key, opts)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.Object), resultError(results[1])
}

func (m *luaStorageMiddleware) GetObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	results, err := m.call(ctx, "GetObject", ctx, bucketName, key, ranges, opts)
	if err != nil {
		return nil, nil, err
	}
	return results[0].(*storage.Object), results[1].([]io.ReadCloser), resultError(results[2])
}

func (m *luaStorageMiddleware) PutObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	results, err := m.call(ctx, "PutObject", ctx, bucketName, key, contentType, data, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.PutObjectResult), resultError(results[1])
}

func (m *luaStorageMiddleware) AppendObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, data io.Reader, checksumInput *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	results, err := m.call(ctx, "AppendObject", ctx, bucketName, key, data, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.AppendObjectResult), resultError(results[1])
}

func (m *luaStorageMiddleware) DeleteObject(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.DeleteObjectOptions) error {
	return oneResult(m.call(ctx, "DeleteObject", ctx, bucketName, key, opts))
}

func (m *luaStorageMiddleware) DeleteObjects(ctx context.Context, bucketName storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	results, err := m.call(ctx, "DeleteObjects", ctx, bucketName, entries)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.DeleteObjectsResult), resultError(results[1])
}

func (m *luaStorageMiddleware) CreateMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, contentType *string, checksumType *string) (*storage.InitiateMultipartUploadResult, error) {
	results, err := m.call(ctx, "CreateMultipartUpload", ctx, bucketName, key, contentType, checksumType)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.InitiateMultipartUploadResult), resultError(results[1])
}

func (m *luaStorageMiddleware) UploadPart(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, partNumber int32, data io.Reader, checksumInput *storage.ChecksumInput) (*storage.UploadPartResult, error) {
	results, err := m.call(ctx, "UploadPart", ctx, bucketName, key, uploadId, partNumber, data, checksumInput)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.UploadPartResult), resultError(results[1])
}

func (m *luaStorageMiddleware) CompleteMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, checksumInput *storage.ChecksumInput, opts *storage.CompleteMultipartUploadOptions) (*storage.CompleteMultipartUploadResult, error) {
	results, err := m.call(ctx, "CompleteMultipartUpload", ctx, bucketName, key, uploadId, checksumInput, opts)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.CompleteMultipartUploadResult), resultError(results[1])
}

func (m *luaStorageMiddleware) AbortMultipartUpload(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId) error {
	return oneResult(m.call(ctx, "AbortMultipartUpload", ctx, bucketName, key, uploadId))
}

func (m *luaStorageMiddleware) ListMultipartUploads(ctx context.Context, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	results, err := m.call(ctx, "ListMultipartUploads", ctx, bucketName, opts)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.ListMultipartUploadsResult), resultError(results[1])
}

func (m *luaStorageMiddleware) ListParts(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, uploadId storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	results, err := m.call(ctx, "ListParts", ctx, bucketName, key, uploadId, opts)
	if err != nil {
		return nil, err
	}
	return results[0].(*storage.ListPartsResult), resultError(results[1])
}
