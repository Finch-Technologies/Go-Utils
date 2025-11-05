package utils

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	rand2 "math/rand/v2"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/finch-technologies/go-utils/log"
	"github.com/google/go-querystring/query"
)

func If[T any](condition bool, trueValue, falseValue T) T {
	if condition {
		return trueValue
	}
	return falseValue
}

func Map[T, V any](ts []T, fn func(T) V) []V {
	result := make([]V, len(ts))
	for i, t := range ts {
		result[i] = fn(t)
	}
	return result
}

func FirstValue(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return values[len(values)-1]
}

func Filter[T any](ss []T, test func(T, int) bool) (res []T) {
	for i, s := range ss {
		if test(s, i) {
			res = append(res, s)
		}
	}
	return
}

func Find[T any](ss []T, test func(T, int) bool) (res T) {
	for i, s := range ss {
		if test(s, i) {
			return s
		}
	}
	return
}

func Foreach[T any](ss []T, apply func(T, int) T) []T {
	result := make([]T, len(ss))
	for i, t := range ss {
		result[i] = apply(t, i)
	}
	return result
}

func Contains[T comparable](s []T, e T) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func StripWhitespace(s string) string {
	result := make([]rune, 0, len(s))
	for _, r := range s {
		if !unicode.IsSpace(r) {
			result = append(result, r)
		}
	}
	return string(result)
}

func StartsWith(s, prefix string) bool {
	s = strings.TrimSpace(s)
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func StripNonAlphanumeric(s string) string {
	return regexp.MustCompile(`[^A-Za-z0-9\-_.]+`).ReplaceAllString(s, "")
}

const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func RandomString(length int) string {
	sb := strings.Builder{}
	sb.Grow(length)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := length-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

func RandomInt(min, max int) int {
	max += 1
	return rand2.IntN(max-min) + min
}

func SleepRandom(ctx context.Context, min, max int) {
	Sleep(ctx, time.Duration(RandomInt(min, max))*time.Millisecond)
}

func Hash(s string, l int) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))

	if l >= len(sha) {
		return sha
	}

	return sha[:l]
}

func EncodeURLParams(q any) string {
	v, _ := query.Values(q)
	return fmt.Sprint("?", v)
}

func Retry[T any](ctx context.Context, retries int, delay time.Duration, f func(retryCount int) (T, error)) (T, error) {
	var err error
	var result T
	for i := 0; i < retries; i++ {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		result, err = f(i)
		if err == nil {
			return result, nil
		}
		Sleep(ctx, delay)
	}
	return result, err
}

// Try wraps a goroutine and will recover from a panic
// If a logger is provided, it will log the error using the given logger
func Try(f func(), logger ...log.LoggerInterface) {
	defer func() {
		if err := recover(); err != nil {
			if len(logger) > 0 {
				logger[0].ErrorStack(string(debug.Stack()), "%v", err)
			}
		}
	}()

	f()
}

// TryReturn wraps a function with a return value and will recover from a panic by returning an error
func TryReturn[T any](f func() (T, error)) (res T, err error) {
	defer func() {
		if r := recover(); r != nil {
			if er, ok := r.(error); ok {
				err = er
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	return f()
}

// TryCatch wraps a goroutine and will recover from a panic
// It will pass the error message to the catch function on panic
func TryCatch(f func(), catch func(e error, stackTrace string)) {
	defer func() {
		if err := recover(); err != nil {
			if _, ok := err.(error); ok {
				catch(err.(error), string(debug.Stack()))
			} else {
				catch(fmt.Errorf("%v", err), string(debug.Stack()))
			}
		}
	}()

	f()
}

func DurationOrDefault(value, defaultValue time.Duration) time.Duration {
	if value == 0 {
		return defaultValue
	}
	return value
}

func StringOrDefault(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

func IntOrDefault(value, defaultValue int) int {
	if value == 0 {
		return defaultValue
	}
	return value
}

func StringToIntOrDefault(value string, defaultValue int) int {
	if value == "" {
		return defaultValue
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return i
}

func Sleep(ctx context.Context, delay time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

func ParseJson[T interface{}](jsonStr string) (T, error) {
	var data T
	re := regexp.MustCompile(`[\t\n\r]`)
	cleanStr := re.ReplaceAllString(jsonStr, "")
	err := json.Unmarshal([]byte(cleanStr), &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

func RegexSubMatch(r *regexp.Regexp, str string) map[string]string {
	match := r.FindStringSubmatch(str)
	subMatchMap := make(map[string]string)
	for i, name := range r.SubexpNames() {
		if i != 0 {
			subMatchMap[name] = match[i]
		}
	}

	return subMatchMap
}

func ParseInt(s string) int {
	value, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return value
}

func ParseTimeout(s string) time.Duration {
	duration, err := time.ParseDuration(s)
	if err != nil {
		return 30 * time.Second // default
	}
	return duration
}

/*
MergeObjects merges two objects of the same type.
It will iterate over the fields of the object and set the value of objA to the value of objB if the value of objA is the zero value.
*/
func MergeObjects[T any](objA *T, objB T) {

	//If objA type is not a pointer to a struct, return
	if reflect.TypeOf(objA).Kind() != reflect.Ptr || reflect.TypeOf(objA).Elem().Kind() != reflect.Struct {
		return
	}

	if objA == nil {
		return
	}

	//Iterate over the fields of the object and set the value to the default value if the value is the zero value
	fields := reflect.TypeOf(objA).Elem()
	objAValue := reflect.ValueOf(objA).Elem()
	objBValue := reflect.ValueOf(objB)

	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		if objAValue.Field(i).Interface() == reflect.Zero(field.Type).Interface() {
			objAValue.Field(i).Set(objBValue.Field(i))
		}
	}
}

// GetContentTypeFromURL tries to infer content type from file extension in URL
func GetContentTypeFromURL(fileURL string) string {
	ext := strings.ToLower(filepath.Ext(fileURL))
	switch ext {
	case ".pdf":
		return "application/pdf"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".tiff", ".tif":
		return "image/tiff"
	case ".bmp":
		return "image/bmp"
	default:
		return "application/octet-stream"
	}
}

// GetExtensionFromContentType returns file extension based on content type
func GetExtensionFromContentType(contentType string) string {
	contentType = strings.ToLower(contentType)
	switch contentType {
	case "application/pdf":
		return ".pdf"
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/tiff":
		return ".tiff"
	case "image/bmp":
		return ".bmp"
	default:
		return ".bin"
	}
}
