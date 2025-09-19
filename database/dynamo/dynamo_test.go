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

func TestGeneric(t *testing.T) {

	_, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeAttributes,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to initialize table: %v", err)
	}

	Put("dynamo.test", "test_generic", Person{
		Name:  "John Doe",
		Email: "john.doe@example.com",
	}, SetOptions{
		Expiration: 1 * time.Minute,
	})

	value, err := Get[Person]("dynamo.test", "test_generic")

	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	fmt.Println("Returned value: ", value)
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
		pk   string
		sk   string
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

	if len(results) != 4 {
		t.Fatalf("Expected 4 items, got %d", len(results))
	}

	fmt.Printf("Query results: %v\n", results)
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
		pk   string
		sk   string
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

	if len(results) != 2 {
		t.Fatalf("Expected 2 items with begins_with 'session_', got %d", len(results))
	}

	// Test equals condition
	results, err = table.Query("user456", QueryOptions{
		SortKeyCondition: QueryConditionEquals,
		SortKey:          "002",
	})
	if err != nil {
		t.Fatalf("Failed to query with equals: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 item with equals '002', got %d", len(results))
	}

	// Test greater than condition
	results, err = table.Query("user456", QueryOptions{
		SortKeyCondition: QueryConditionGreaterThan,
		SortKey:          "002",
	})
	if err != nil {
		t.Fatalf("Failed to query with greater than: %v", err)
	}

	// Should get "003", "session_a", "session_b" (3 items)
	if len(results) < 1 {
		t.Fatalf("Expected at least 1 item with greater than '002', got %d", len(results))
	}

	fmt.Printf("Begins_with results: %d, Equals results: %d, Greater than results: %d\n",
		2, 1, len(results))
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

	if len(results) != 3 {
		t.Fatalf("Expected 3 Person items, got %d", len(results))
	}

	// Verify first result is a *Person
	firstPerson, ok := results[0].(*Person)
	if !ok {
		t.Fatalf("Expected *Person, got %T", results[0])
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

	// Should only get the non-expired item
	if len(results) != 1 {
		t.Fatalf("Expected 1 non-expired item, got %d", len(results))
	}

	// Verify it's the valid data
	if results[0] != "valid_data" {
		t.Fatalf("Expected 'valid_data', got %s", results[0])
	}

	fmt.Printf("Non-expired query results: %v\n", results)
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

func TestUpdateAttributes(t *testing.T) {
	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeAttributes,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// First, create an initial person
	original := Person{
		Name:  "John Doe",
		Email: "john@example.com",
	}

	err = table.Put("update_test", original, SetOptions{
		SortKey:    "person1",
		Expiration: 1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Failed to put initial person: %v", err)
	}

	// Now update only the email field
	update := struct {
		Email string `dynamodbav:"email"`
	}{
		Email: "john.doe@newcompany.com",
	}

	err = table.Update("update_test", update, SetOptions{
		SortKey: "person1",
	})
	if err != nil {
		t.Fatalf("Failed to update person: %v", err)
	}

	// Retrieve the updated item
	result, err := table.Get("update_test", GetOptions{
		SortKey: "person1",
		Result:  &Person{},
	})
	if err != nil {
		t.Fatalf("Failed to get updated person: %v", err)
	}

	updatedPerson := result.(*Person)

	// Verify the email was updated but name remained the same
	if updatedPerson.Email != "john.doe@newcompany.com" {
		t.Fatalf("Expected email 'john.doe@newcompany.com', got %s", updatedPerson.Email)
	}

	if updatedPerson.Name != "John Doe" {
		t.Fatalf("Expected name 'John Doe' to remain unchanged, got %s", updatedPerson.Name)
	}

	fmt.Printf("Updated person: %+v\n", updatedPerson)
}

func TestUpdateWithTTL(t *testing.T) {
	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeAttributes,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create initial item
	initial := Person{
		Name:  "Jane Smith",
		Email: "jane@example.com",
	}

	err = table.Put("ttl_update_test", initial, SetOptions{
		SortKey:    "person2",
		Expiration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Failed to put initial item: %v", err)
	}

	// Update with new TTL
	update := struct {
		Name string `dynamodbav:"name"`
	}{
		Name: "Jane Johnson",
	}

	err = table.Update("ttl_update_test", update, SetOptions{
		SortKey:    "person2",
		Expiration: 10 * time.Second, // Short expiration for testing
	})
	if err != nil {
		t.Fatalf("Failed to update with TTL: %v", err)
	}

	// Verify the update worked immediately
	result, err := table.Get("ttl_update_test", GetOptions{
		SortKey: "person2",
		Result:  &Person{},
	})
	if err != nil {
		t.Fatalf("Failed to get updated item: %v", err)
	}

	updatedPerson := result.(*Person)
	if updatedPerson.Name != "Jane Johnson" {
		t.Fatalf("Expected name 'Jane Johnson', got %s", updatedPerson.Name)
	}

	fmt.Printf("Updated item with TTL: %+v\n", updatedPerson)
}

func TestUpdateMultipleFields(t *testing.T) {
	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeAttributes,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create initial item
	initial := Person{
		Name:  "Bob Wilson",
		Email: "bob@example.com",
	}

	err = table.Put("multi_update_test", initial, SetOptions{
		SortKey:    "person3",
		Expiration: 1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Failed to put initial item: %v", err)
	}

	// Update both name and email
	update := Person{
		Name:  "Robert Wilson Jr.",
		Email: "robert.wilson@newcompany.com",
	}

	err = table.Update("multi_update_test", update, SetOptions{
		SortKey: "person3",
	})
	if err != nil {
		t.Fatalf("Failed to update multiple fields: %v", err)
	}

	// Verify both fields were updated
	result, err := table.Get("multi_update_test", GetOptions{
		SortKey: "person3",
		Result:  &Person{},
	})
	if err != nil {
		t.Fatalf("Failed to get updated item: %v", err)
	}

	updatedPerson := result.(*Person)

	if updatedPerson.Name != "Robert Wilson Jr." {
		t.Fatalf("Expected name 'Robert Wilson Jr.', got %s", updatedPerson.Name)
	}

	if updatedPerson.Email != "robert.wilson@newcompany.com" {
		t.Fatalf("Expected email 'robert.wilson@newcompany.com', got %s", updatedPerson.Email)
	}

	fmt.Printf("Updated multiple fields: %+v\n", updatedPerson)
}

func TestUpdateNonExistentItem(t *testing.T) {
	table, err := New(DbOptions{
		TableName:        "dynamo.test",
		ValueStoreMode:   ValueStoreModeAttributes,
		SortKeyAttribute: "group_id",
	})

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Try to update a non-existent item
	update := Person{
		Name:  "Non Existent",
		Email: "nonexistent@example.com",
	}

	err = table.Update("non_existent_key", update, SetOptions{
		SortKey: "missing",
	})

	// Update should succeed even if item doesn't exist (DynamoDB behavior)
	// It will create the item with the update values
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify the item was created
	result, err := table.Get("non_existent_key", GetOptions{
		SortKey: "missing",
		Result:  &Person{},
	})
	if err != nil {
		t.Fatalf("Failed to get created item: %v", err)
	}

	createdPerson := result.(*Person)

	if createdPerson.Name != "Non Existent" {
		t.Fatalf("Expected name 'Non Existent', got %s", createdPerson.Name)
	}

	if createdPerson.Email != "nonexistent@example.com" {
		t.Fatalf("Expected email 'nonexistent@example.com', got %s", createdPerson.Email)
	}

	fmt.Printf("Created item via update: %+v\n", createdPerson)
}
