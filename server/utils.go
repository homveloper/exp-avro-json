package main

import (
	"encoding/json"
	"reflect"
)

// structToMap converts a struct to map[string]interface{} for goavro compatibility
func structToMap(s interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	
	// Use JSON marshal/unmarshal as a simple way to convert struct to map
	// This handles pointer fields (*string) correctly by converting nil to null
	jsonBytes, err := json.Marshal(s)
	if err != nil {
		return result
	}
	
	err = json.Unmarshal(jsonBytes, &result)
	if err != nil {
		return result
	}
	
	return result
}

// mapToStruct converts map[string]interface{} to struct
func mapToStruct(m map[string]interface{}, s interface{}) error {
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		return err
	}
	
	return json.Unmarshal(jsonBytes, s)
}

// getStructSchema generates Avro schema from Go struct using reflection
func getStructSchema(s interface{}) string {
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	
	// This is a simplified version - in production you'd want a more robust schema generator
	// For now, we'll use the predefined schemas
	return ""
}