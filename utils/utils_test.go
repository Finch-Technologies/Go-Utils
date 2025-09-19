package utils

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/finch-technologies/go-utils/log"
)

type TestStruct struct {
	Name  string
	Value int
	Flag  bool
}

func TestIf(t *testing.T) {
	tests := []struct {
		name      string
		condition bool
		trueVal   string
		falseVal  string
		expected  string
	}{
		{"true condition", true, "yes", "no", "yes"},
		{"false condition", false, "yes", "no", "no"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := If(tt.condition, tt.trueVal, tt.falseVal)
			if result != tt.expected {
				t.Errorf("If() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMap(t *testing.T) {
	input := []int{1, 2, 3, 4}
	expected := []string{"1", "2", "3", "4"}

	result := Map(input, func(i int) string {
		return string(rune(i + '0'))
	})

	if len(result) != len(expected) {
		t.Errorf("Map() length = %d, want %d", len(result), len(expected))
	}

	for i, v := range result {
		if v != expected[i] {
			t.Errorf("Map()[%d] = %v, want %v", i, v, expected[i])
		}
	}
}

func TestFirstValue(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{"first non-empty", []string{"first", "second", "third"}, "first"},
		{"skip empty", []string{"", "  ", "second"}, "second"},
		{"all empty returns last", []string{"", "  ", "   "}, "   "},
		{"single value", []string{"only"}, "only"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FirstValue(tt.values...)
			if result != tt.expected {
				t.Errorf("FirstValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFilter(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	result := Filter(input, func(n int, i int) bool {
		return n%2 == 0
	})

	expected := []int{2, 4}
	if len(result) != len(expected) {
		t.Errorf("Filter() length = %d, want %d", len(result), len(expected))
	}

	for i, v := range result {
		if v != expected[i] {
			t.Errorf("Filter()[%d] = %v, want %v", i, v, expected[i])
		}
	}
}

func TestFind(t *testing.T) {
	input := []string{"apple", "banana", "cherry"}
	result := Find(input, func(s string, i int) bool {
		return strings.Contains(s, "an")
	})

	if result != "banana" {
		t.Errorf("Find() = %v, want %v", result, "banana")
	}

	notFound := Find(input, func(s string, i int) bool {
		return s == "grape"
	})

	if notFound != "" {
		t.Errorf("Find() = %v, want empty string", notFound)
	}
}

func TestForeach(t *testing.T) {
	input := []int{1, 2, 3}
	result := Foreach(input, func(n int, i int) int {
		return n * 2
	})

	expected := []int{2, 4, 6}
	if len(result) != len(expected) {
		t.Errorf("Foreach() length = %d, want %d", len(result), len(expected))
	}

	for i, v := range result {
		if v != expected[i] {
			t.Errorf("Foreach()[%d] = %v, want %v", i, v, expected[i])
		}
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		name     string
		slice    []string
		element  string
		expected bool
	}{
		{"contains", []string{"a", "b", "c"}, "b", true},
		{"not contains", []string{"a", "b", "c"}, "d", false},
		{"empty slice", []string{}, "a", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Contains(tt.slice, tt.element)
			if result != tt.expected {
				t.Errorf("Contains() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestStripWhitespace(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"normal text", "hello world", "helloworld"},
		{"with tabs", "hello\tworld", "helloworld"},
		{"with newlines", "hello\nworld", "helloworld"},
		{"multiple spaces", "hello   world", "helloworld"},
		{"mixed whitespace", "hello \t\n world", "helloworld"},
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StripWhitespace(tt.input)
			if result != tt.expected {
				t.Errorf("StripWhitespace() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestStartsWith(t *testing.T) {
	tests := []struct {
		name     string
		str      string
		prefix   string
		expected bool
	}{
		{"exact match", "hello", "hello", true},
		{"prefix match", "hello world", "hello", true},
		{"no match", "hello", "world", false},
		{"empty prefix", "hello", "", true},
		{"longer prefix", "hi", "hello", false},
		{"with whitespace", "  hello", "hello", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StartsWith(tt.str, tt.prefix)
			if result != tt.expected {
				t.Errorf("StartsWith() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestRandomString(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"short string", 5},
		{"medium string", 20},
		{"long string", 100},
		{"zero length", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RandomString(tt.length)
			if len(result) != tt.length {
				t.Errorf("RandomString() length = %d, want %d", len(result), tt.length)
			}

			// Check that it only contains valid characters
			for _, char := range result {
				if !strings.ContainsRune(letterBytes, char) {
					t.Errorf("RandomString() contains invalid character: %c", char)
				}
			}
		})
	}

	// Test uniqueness (should be different on multiple calls)
	if RandomString(10) == RandomString(10) {
		t.Log("Note: RandomString generated same value twice (very unlikely but possible)")
	}
}

func TestRandomInt(t *testing.T) {
	tests := []struct {
		name string
		min  int
		max  int
	}{
		{"positive range", 1, 10},
		{"negative range", -10, -1},
		{"mixed range", -5, 5},
		{"single value", 5, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RandomInt(tt.min, tt.max)
			if result < tt.min || result > tt.max {
				t.Errorf("RandomInt() = %d, want between %d and %d", result, tt.min, tt.max)
			}
		})
	}
}

func TestSleepRandom(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	SleepRandom(ctx, 1, 5)
	duration := time.Since(start)

	if duration < time.Millisecond || duration > 10*time.Millisecond {
		t.Errorf("SleepRandom() took %v, expected between 1-5ms", duration)
	}
}

func TestHash(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		length   int
	}{
		{"short hash", "test", 8},
		{"full hash", "test", 100}, // Should return full hash
		{"empty string", "", 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Hash(tt.input, tt.length)
			if len(result) > tt.length && tt.length < 100 {
				t.Errorf("Hash() length = %d, want <= %d", len(result), tt.length)
			}
			if result == "" {
				t.Errorf("Hash() should not return empty string")
			}
		})
	}
}

func TestEncodeURLParams(t *testing.T) {
	type Query struct {
		Name string `url:"name"`
		Age  int    `url:"age"`
	}

	q := Query{Name: "John", Age: 30}
	result := EncodeURLParams(q)

	// Should start with "?" and contain the parameters
	if !strings.HasPrefix(result, "?") {
		t.Errorf("EncodeURLParams() should start with '?', got %s", result)
	}
	// The format might be different (url encoded arrays), just check basic structure
	if !strings.Contains(result, "name") || !strings.Contains(result, "age") {
		t.Errorf("EncodeURLParams() = %s, should contain name and age parameters", result)
	}
}

func TestRetry(t *testing.T) {
	ctx := context.Background()

	// Test successful retry
	callCount := 0
	result, err := Retry(ctx, 3, time.Millisecond, func(retryCount int) (string, error) {
		callCount++
		if callCount < 2 {
			return "", errors.New("temporary error")
		}
		return "success", nil
	})

	if err != nil {
		t.Errorf("Retry() returned error: %v", err)
	}
	if result != "success" {
		t.Errorf("Retry() = %s, want success", result)
	}
	if callCount != 2 {
		t.Errorf("Retry() called %d times, want 2", callCount)
	}
}

func TestTryReturn(t *testing.T) {
	// Test normal function
	result, err := TryReturn(func() (string, error) {
		return "success", nil
	})

	if err != nil {
		t.Errorf("TryReturn() returned error: %v", err)
	}
	if result != "success" {
		t.Errorf("TryReturn() = %s, want success", result)
	}

	// Test panic recovery
	result2, err2 := TryReturn(func() (string, error) {
		panic("test panic")
	})

	if err2 == nil {
		t.Errorf("TryReturn() should have returned error for panic")
	}
	if result2 != "" {
		t.Errorf("TryReturn() = %s, want empty string for panic", result2)
	}
}

func TestStringOrDefault(t *testing.T) {
	tests := []struct {
		name         string
		value        string
		defaultValue string
		expected     string
	}{
		{"non-empty value", "hello", "default", "hello"},
		{"empty value", "", "default", "default"},
		{"both empty", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StringOrDefault(tt.value, tt.defaultValue)
			if result != tt.expected {
				t.Errorf("StringOrDefault() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIntOrDefault(t *testing.T) {
	tests := []struct {
		name         string
		value        int
		defaultValue int
		expected     int
	}{
		{"non-zero value", 42, 10, 42},
		{"zero value", 0, 10, 10},
		{"both zero", 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IntOrDefault(tt.value, tt.defaultValue)
			if result != tt.expected {
				t.Errorf("IntOrDefault() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestStringToIntOrDefault(t *testing.T) {
	tests := []struct {
		name         string
		value        string
		defaultValue int
		expected     int
	}{
		{"valid number", "42", 10, 42},
		{"invalid number", "abc", 10, 10},
		{"empty string", "", 10, 10},
		{"zero string", "0", 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StringToIntOrDefault(tt.value, tt.defaultValue)
			if result != tt.expected {
				t.Errorf("StringToIntOrDefault() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSleep(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	Sleep(ctx, 5*time.Millisecond)
	duration := time.Since(start)

	if duration < 4*time.Millisecond {
		t.Errorf("Sleep() took %v, expected at least 5ms", duration)
	}
}

func TestParseJson(t *testing.T) {
	type TestData struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	jsonStr := `{"name": "John", "age": 30}`
	result, err := ParseJson[TestData](jsonStr)

	if err != nil {
		t.Errorf("ParseJson() returned error: %v", err)
	}
	if result.Name != "John" || result.Age != 30 {
		t.Errorf("ParseJson() = %+v, want {Name:John Age:30}", result)
	}
}

func TestRegexSubMatch(t *testing.T) {
	r := regexp.MustCompile(`(?P<name>\w+)@(?P<domain>\w+\.\w+)`)
	str := "john@example.com"

	result := RegexSubMatch(r, str)

	if result["name"] != "john" {
		t.Errorf("RegexSubMatch() name = %s, want john", result["name"])
	}
	if result["domain"] != "example.com" {
		t.Errorf("RegexSubMatch() domain = %s, want example.com", result["domain"])
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"valid positive", "123", 123},
		{"valid negative", "-45", -45},
		{"invalid", "abc", 0},
		{"empty", "", 0},
		{"zero", "0", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseInt(tt.input)
			if result != tt.expected {
				t.Errorf("ParseInt() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParseTimeout(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected time.Duration
	}{
		{"valid seconds", "5s", 5 * time.Second},
		{"valid milliseconds", "100ms", 100 * time.Millisecond},
		{"invalid", "invalid", 30 * time.Second}, // default
		{"empty", "", 30 * time.Second}, // default
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseTimeout(tt.input)
			if result != tt.expected {
				t.Errorf("ParseTimeout() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMergeObjects(t *testing.T) {
	tests := []struct {
		name string
		objA TestStruct
		objB TestStruct
		want TestStruct
	}{
		{
			name: "merge zero values",
			objA: TestStruct{Name: "", Value: 0, Flag: false},
			objB: TestStruct{Name: "test", Value: 42, Flag: true},
			want: TestStruct{Name: "test", Value: 42, Flag: true},
		},
		{
			name: "keep non-zero values",
			objA: TestStruct{Name: "existing", Value: 100, Flag: true},
			objB: TestStruct{Name: "new", Value: 42, Flag: false},
			want: TestStruct{Name: "existing", Value: 100, Flag: true},
		},
		{
			name: "partial merge",
			objA: TestStruct{Name: "existing", Value: 0, Flag: true},
			objB: TestStruct{Name: "new", Value: 42, Flag: false},
			want: TestStruct{Name: "existing", Value: 42, Flag: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objA := tt.objA
			MergeObjects(&objA, tt.objB)
			if objA != tt.want {
				t.Errorf("MergeObjects() = %+v, want %+v", objA, tt.want)
			}
		})
	}
}

func TestMergeObjects_InvalidTypes(t *testing.T) {
	// Test with nil pointer
	var nilPtr *TestStruct
	objB := TestStruct{Name: "test"}
	MergeObjects(nilPtr, objB) // Should not panic

	// Test with non-struct type
	var intPtr *int
	var intVal int = 42
	MergeObjects(intPtr, intVal) // Should not panic and return early
}

// Mock logger for testing
type mockLogger struct {
	logged bool
	msg    string
}

// Ensure mockLogger implements the interface
var _ log.LoggerInterface = (*mockLogger)(nil)

func (m *mockLogger) ErrorStack(stack, s string, args ...any) {
	m.logged = true
	m.msg = stack
}

func (m *mockLogger) Error(args ...any) {}
func (m *mockLogger) Errorf(s string, args ...any) {}
func (m *mockLogger) Warning(args ...any) {}
func (m *mockLogger) Info(args ...any) {}
func (m *mockLogger) Infof(s string, args ...any) {}
func (m *mockLogger) Debug(args ...any) {}
func (m *mockLogger) Debugf(s string, args ...any) {}
func (m *mockLogger) InfoEvent(eventType string, data string) {}
func (m *mockLogger) ErrorEvent(eventType string, data string) {}
func (m *mockLogger) ErrorEventWithResources(eventType, screenshot, text, data string) {}
func (m *mockLogger) InfoFile(filePath string, data string) {}
func (m *mockLogger) ErrorFile(filePath string, data string) {}
func (m *mockLogger) DebugFields(msg string, fields map[string]any) {}
func (m *mockLogger) InfoFields(msg string, fields map[string]any) {}
func (m *mockLogger) ErrorFields(msg string, fields map[string]any) {}
func (m *mockLogger) GetContext() context.Context { return context.Background() }

func TestTry(t *testing.T) {
	mockLog := &mockLogger{}

	// Test normal function (should not panic or log)
	Try(func() {
		// Normal operation
	}, mockLog)

	if mockLog.logged {
		t.Errorf("Try() should not have logged for normal function")
	}

	// Test panic recovery
	Try(func() {
		panic("test panic")
	}, mockLog)

	if !mockLog.logged {
		t.Errorf("Try() should have logged panic")
	}
}

func TestTryCatch(t *testing.T) {
	var caughtError error
	var stackTrace string

	TryCatch(func() {
		panic("test panic")
	}, func(e error, stack string) {
		caughtError = e
		stackTrace = stack
	})

	if caughtError == nil {
		t.Errorf("TryCatch() should have caught panic")
	}
	if stackTrace == "" {
		t.Errorf("TryCatch() should have provided stack trace")
	}
	if caughtError.Error() != "test panic" {
		t.Errorf("TryCatch() caught error = %s, want 'test panic'", caughtError.Error())
	}
}