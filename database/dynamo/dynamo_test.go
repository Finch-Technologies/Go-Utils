package dynamo

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

type Person struct {
	Name  string `json:"name" dynamodbav:"name"`
	Email string `json:"email" dynamodbav:"email"`
}

func TestGetString(t *testing.T) {

	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeJson,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = table.Put("test_string", "test", SetOptions{
		Expiration: 1 * time.Minute,
	})

	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	value, err := table.Get("test_string")

	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	fmt.Println("Returned value: ", value)

	expected := "test"

	if value != expected {
		t.Fatalf("Expected %s, got %s", expected, value)
	}
}

func TestGetJson(t *testing.T) {

	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeJson,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	s := Person{
		Name:  "John Doe",
		Email: "john.doe@example.com",
	}

	err = table.Put("test_json", s, SetOptions{
		Expiration: 1 * time.Minute,
	})

	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	value, err := table.Get("test_json")

	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	fmt.Println("Returned value: ", value)

	expectedJson := `{"name":"John Doe","email":"john.doe@example.com"}`

	if value != expectedJson {
		t.Fatalf("Expected %s, got %s", expectedJson, value)
	}
}

func TestGetAttributes(t *testing.T) {

	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeAttributes,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	expected := Person{
		Name:  "John Doe",
		Email: "john.doe@example.com",
	}

	err = table.Put("test_attributes", expected, SetOptions{
		Expiration: 1 * time.Minute,
	})

	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	value, err := table.Get("test_attributes", GetOptions{
		Result: &Person{},
	})

	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	fmt.Println("Returned value: ", value)

	person := value.(*Person)

	if person.Name != expected.Name || person.Email != expected.Email {
		t.Fatalf("Expected %v, got %v", expected, value)
	}
}

func TestGetJsonWithExpiry(t *testing.T) {

	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeJson,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = table.Put("test_json_with_expiry", Person{
		Name:  "John Doe",
		Email: "john.doe@example.com",
	}, SetOptions{
		Expiration: 5 * time.Second,
	})

	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	time.Sleep(6 * time.Second)

	value, err := table.Get("test_json_with_expiry")

	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	fmt.Println(value)

	expected := ""

	if value != expected {
		t.Fatalf("Expected %s, got %s", expected, value)
	}
}

func TestQueryBasic(t *testing.T) {
	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeJson,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple items with same partition key but different sort keys
	testItems := []struct {
		pk string
		sk string
		data string
	}{
		{"user123", "profile", "profile_data"},
		{"user123", "settings", "settings_data"},
		{"user123", "session_abc", "session_data_1"},
		{"user123", "session_def", "session_data_2"},
	}

	for _, item := range testItems {
		err = table.Put(item.pk, item.data, SetOptions{
			SortKey:    item.sk,
			Expiration: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("Failed to put item %s/%s: %v", item.pk, item.sk, err)
		}
	}

	// Query all items for user123
	results, err := table.Query("user123")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	resultsList, ok := results.([]interface{})
	if !ok {
		t.Fatalf("Expected []interface{}, got %T", results)
	}

	if len(resultsList) != 4 {
		t.Fatalf("Expected 4 items, got %d", len(resultsList))
	}

	fmt.Printf("Query results: %v\n", resultsList)
}

func TestQueryWithSortKeyConditions(t *testing.T) {
	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeJson,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test items with numeric sort keys for comparison tests
	testItems := []struct {
		pk string
		sk string
		data string
	}{
		{"user456", "001", "data_1"},
		{"user456", "002", "data_2"},
		{"user456", "003", "data_3"},
		{"user456", "session_a", "session_a_data"},
		{"user456", "session_b", "session_b_data"},
	}

	for _, item := range testItems {
		err = table.Put(item.pk, item.data, SetOptions{
			SortKey:    item.sk,
			Expiration: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("Failed to put item %s/%s: %v", item.pk, item.sk, err)
		}
	}

	// Test begins_with condition
	results, err := table.Query("user456", QueryOptions{
		SortKeyCondition: QueryConditionBeginsWith,
		SortKey:          "session_",
	})
	if err != nil {
		t.Fatalf("Failed to query with begins_with: %v", err)
	}

	resultsList := results.([]interface{})
	if len(resultsList) != 2 {
		t.Fatalf("Expected 2 items with begins_with 'session_', got %d", len(resultsList))
	}

	// Test equals condition
	results, err = table.Query("user456", QueryOptions{
		SortKeyCondition: QueryConditionEquals,
		SortKey:          "002",
	})
	if err != nil {
		t.Fatalf("Failed to query with equals: %v", err)
	}

	resultsList = results.([]interface{})
	if len(resultsList) != 1 {
		t.Fatalf("Expected 1 item with equals '002', got %d", len(resultsList))
	}

	// Test greater than condition
	results, err = table.Query("user456", QueryOptions{
		SortKeyCondition: QueryConditionGreaterThan,
		SortKey:          "002",
	})
	if err != nil {
		t.Fatalf("Failed to query with greater than: %v", err)
	}

	resultsList = results.([]interface{})
	// Should get "003", "session_a", "session_b" (3 items)
	if len(resultsList) < 1 {
		t.Fatalf("Expected at least 1 item with greater than '002', got %d", len(resultsList))
	}

	fmt.Printf("Begins_with results: %d, Equals results: %d, Greater than results: %d\n",
		2, 1, len(resultsList))
}

func TestQueryWithAttributes(t *testing.T) {
	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeAttributes,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple Person objects
	testPersons := []struct {
		pk   string
		sk   string
		data Person
	}{
		{"company1", "emp001", Person{Name: "John Doe", Email: "john@company.com"}},
		{"company1", "emp002", Person{Name: "Jane Smith", Email: "jane@company.com"}},
		{"company1", "emp003", Person{Name: "Bob Johnson", Email: "bob@company.com"}},
	}

	for _, item := range testPersons {
		err = table.Put(item.pk, item.data, SetOptions{
			SortKey:    item.sk,
			Expiration: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("Failed to put person %s/%s: %v", item.pk, item.sk, err)
		}
	}

	// Query with Person result type
	results, err := table.Query("company1", QueryOptions{
		Result: &Person{},
	})
	if err != nil {
		t.Fatalf("Failed to query with Person result: %v", err)
	}

	resultsList, ok := results.([]interface{})
	if !ok {
		t.Fatalf("Expected []interface{}, got %T", results)
	}

	if len(resultsList) != 3 {
		t.Fatalf("Expected 3 Person items, got %d", len(resultsList))
	}

	// Verify first result is a *Person
	firstPerson, ok := resultsList[0].(*Person)
	if !ok {
		t.Fatalf("Expected *Person, got %T", resultsList[0])
	}

	if firstPerson.Name == "" || firstPerson.Email == "" {
		t.Fatalf("Person data not properly unmarshaled: %+v", firstPerson)
	}

	fmt.Printf("Query attributes result: %+v\n", firstPerson)
}

func TestQueryWithExpiredItems(t *testing.T) {
	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeJson,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert items with short expiration
	err = table.Put("user789", "valid_data", SetOptions{
		SortKey:    "valid",
		Expiration: 1 * time.Minute, // Valid for a minute
	})
	if err != nil {
		t.Fatalf("Failed to put valid item: %v", err)
	}

	err = table.Put("user789", "expired_data", SetOptions{
		SortKey:    "expired",
		Expiration: 1 * time.Second, // Will expire soon
	})
	if err != nil {
		t.Fatalf("Failed to put expired item: %v", err)
	}

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// Query should only return valid items
	results, err := table.Query("user789")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	resultsList := results.([]interface{})

	// Should only get the non-expired item
	if len(resultsList) != 1 {
		t.Fatalf("Expected 1 non-expired item, got %d", len(resultsList))
	}

	// Verify it's the valid data
	if resultsList[0] != "valid_data" {
		t.Fatalf("Expected 'valid_data', got %s", resultsList[0])
	}

	fmt.Printf("Non-expired query results: %v\n", resultsList)
}

func TestQueryInvalidSortKeyCondition(t *testing.T) {
	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeJson,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test with invalid/unsupported condition
	_, err = table.Query("user999", QueryOptions{
		SortKeyCondition: "invalid_condition",
		SortKey:          "test",
	})

	if err == nil {
		t.Fatalf("Expected error for invalid sort key condition, got nil")
	}

	if !strings.Contains(fmt.Sprintf("%v", err), "unsupported sort key condition") {
		t.Fatalf("Expected 'unsupported sort key condition' error, got: %v", err)
	}

	fmt.Printf("Correctly caught invalid condition error: %v\n", err)
}
