package main

import (
	"encoding/json"
)

// convertToAvroMap converts interface{} to Avro-compatible map format
func convertToAvroMap(data interface{}) map[string]string {
	result := make(map[string]string)

	// Convert the data to JSON first, then to map[string]interface{}
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return result
	}

	var dataMap map[string]interface{}
	err = json.Unmarshal(jsonBytes, &dataMap)
	if err != nil {
		return result
	}

	// Convert all values to strings for Avro compatibility
	for key, value := range dataMap {
		switch v := value.(type) {
		case string:
			result[key] = v
		case nil:
			result[key] = ""
		default:
			// Convert any other type to JSON string
			valueBytes, _ := json.Marshal(v)
			result[key] = string(valueBytes)
		}
	}

	return result
}
