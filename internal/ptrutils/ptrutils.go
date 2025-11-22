package ptrutils

func ToPtr[T any](val T) *T {
	return &val
}

func MapPtr[T any, E any](ptr *T, fn func(T) E) *E {
	if ptr == nil {
		return nil
	}
	mappedVal := fn(*ptr)
	return &mappedVal
}
