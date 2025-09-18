package dynamo

import (
	"fmt"
	"testing"
	"time"
)

type Person struct {
	Name  string `json:"name" dynamodbav:"name"`
	Email string `json:"email" dynamodbav:"email"`
}

func TestGet(t *testing.T) {

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

	err = table.Set("test", s, SetOptions{
		Expiration: 1 * time.Minute,
	})

	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	value, err := table.Get("test", GetOptions{
		Result: &Person{},
	})

	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	fmt.Println(value)
}
