package sliceutils

func Map[S any, T any](f func(s S) T, sourceArray []S) []T {
	targetArray := []T{}
	for _, sourceElement := range sourceArray {
		targetElement := f(sourceElement)
		targetArray = append(targetArray, targetElement)
	}
	return targetArray
}

func RemoveByIndex[T any](s []T, i int) []T {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}
