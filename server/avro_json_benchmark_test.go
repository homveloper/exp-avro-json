// 벤치마크 테스트 실행 방법:
// 1. 모든 벤치마크 실행: go test -run=^$ -bench=. -benchmem
// 2. 특정 벤치마크 실행: go test -run=^$ -bench=BenchmarkStandardJSON20Characters -benchmem
// 3. 3회 반복 실행: go test -run=^$ -bench=. -benchmem -count=3
// 4. 메모리 프로파일: go test -run=^$ -bench=BenchmarkStandardJSON20Characters -memprofile=mem.out
// 5. 프로파일 분석: go tool pprof -top mem.out

package main

import (
	"encoding/json"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/linkedin/goavro/v2"
)

type UserCharacterStorage struct {
	UserID     string      `json:"user_id"`
	Characters []Character `json:"characters"`
}

type Character struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Level      int       `json:"level"`
	Experience int       `json:"experience"`
	Stats      Stats     `json:"stats"`
	Inventory  []Item    `json:"inventory"`
	Skills     []Skill   `json:"skills"`
	Equipment  Equipment `json:"equipment"`
	Quests     []Quest   `json:"quests"`
	Metadata   Metadata  `json:"metadata"`
}

type Stats struct {
	Health   int `json:"health"`
	Mana     int `json:"mana"`
	Strength int `json:"strength"`
	Defense  int `json:"defense"`
	Agility  int `json:"agility"`
	Magic    int `json:"magic"`
}

type Item struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Quantity int    `json:"quantity"`
	Rarity   string `json:"rarity"`
}

type Skill struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Level    int    `json:"level"`
	Cooldown int    `json:"cooldown"`
}

type Equipment struct {
	Weapon    string `json:"weapon"`
	Armor     string `json:"armor"`
	Accessory string `json:"accessory"`
}

type Quest struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Progress int    `json:"progress"`
	Status   string `json:"status"`
}

type Metadata struct {
	CreatedAt    string `json:"created_at"`
	LastModified string `json:"last_modified"`
	PlayTime     int    `json:"play_time"`
}

const userCharacterSchema = `{
	"type": "record",
	"name": "UserCharacterStorage",
	"fields": [
		{"name": "user_id", "type": "string"},
		{
			"name": "characters",
			"type": {
				"type": "array",
				"items": {
					"type": "record",
					"name": "Character",
					"fields": [
						{"name": "id", "type": "string"},
						{"name": "name", "type": "string"},
						{"name": "level", "type": "int"},
						{"name": "experience", "type": "int"},
						{
							"name": "stats",
							"type": {
								"type": "record",
								"name": "Stats",
								"fields": [
									{"name": "health", "type": "int"},
									{"name": "mana", "type": "int"},
									{"name": "strength", "type": "int"},
									{"name": "defense", "type": "int"},
									{"name": "agility", "type": "int"},
									{"name": "magic", "type": "int"}
								]
							}
						},
						{
							"name": "inventory",
							"type": {
								"type": "array",
								"items": {
									"type": "record",
									"name": "Item",
									"fields": [
										{"name": "id", "type": "string"},
										{"name": "name", "type": "string"},
										{"name": "type", "type": "string"},
										{"name": "quantity", "type": "int"},
										{"name": "rarity", "type": "string"}
									]
								}
							}
						},
						{
							"name": "skills",
							"type": {
								"type": "array",
								"items": {
									"type": "record",
									"name": "Skill",
									"fields": [
										{"name": "id", "type": "string"},
										{"name": "name", "type": "string"},
										{"name": "level", "type": "int"},
										{"name": "cooldown", "type": "int"}
									]
								}
							}
						},
						{
							"name": "equipment",
							"type": {
								"type": "record",
								"name": "Equipment",
								"fields": [
									{"name": "weapon", "type": "string"},
									{"name": "armor", "type": "string"},
									{"name": "accessory", "type": "string"}
								]
							}
						},
						{
							"name": "quests",
							"type": {
								"type": "array",
								"items": {
									"type": "record",
									"name": "Quest",
									"fields": [
										{"name": "id", "type": "string"},
										{"name": "name", "type": "string"},
										{"name": "progress", "type": "int"},
										{"name": "status", "type": "string"}
									]
								}
							}
						},
						{
							"name": "metadata",
							"type": {
								"type": "record",
								"name": "Metadata",
								"fields": [
									{"name": "created_at", "type": "string"},
									{"name": "last_modified", "type": "string"},
									{"name": "play_time", "type": "int"}
								]
							}
						}
					]
				}
			}
		}
	]
}`

// gofakeit을 사용하여 더미 캐릭터 데이터 생성
func generateDummyCharacters(count int) UserCharacterStorage {
	storage := UserCharacterStorage{
		UserID:     gofakeit.UUID(),
		Characters: make([]Character, count),
	}

	for i := 0; i < count; i++ {
		char := Character{
			ID:         gofakeit.UUID(),
			Name:       gofakeit.Username(),
			Level:      gofakeit.Number(1, 100),
			Experience: gofakeit.Number(0, 100000),
			Stats: Stats{
				Health:   gofakeit.Number(100, 10000),
				Mana:     gofakeit.Number(50, 5000),
				Strength: gofakeit.Number(10, 100),
				Defense:  gofakeit.Number(10, 100),
				Agility:  gofakeit.Number(10, 100),
				Magic:    gofakeit.Number(10, 100),
			},
			Equipment: Equipment{
				Weapon:    gofakeit.Word(),
				Armor:     gofakeit.Word(),
				Accessory: gofakeit.Word(),
			},
			Metadata: Metadata{
				CreatedAt:    gofakeit.Date().Format("2006-01-02 15:04:05"),
				LastModified: gofakeit.Date().Format("2006-01-02 15:04:05"),
				PlayTime:     gofakeit.Number(0, 10000),
			},
		}

		// Generate inventory
		itemCount := gofakeit.Number(5, 20)
		char.Inventory = make([]Item, itemCount)
		for j := 0; j < itemCount; j++ {
			char.Inventory[j] = Item{
				ID:       gofakeit.UUID(),
				Name:     gofakeit.Word(),
				Type:     gofakeit.RandomString([]string{"weapon", "armor", "consumable", "material"}),
				Quantity: gofakeit.Number(1, 99),
				Rarity:   gofakeit.RandomString([]string{"common", "rare", "epic", "legendary"}),
			}
		}

		// Generate skills
		skillCount := gofakeit.Number(3, 10)
		char.Skills = make([]Skill, skillCount)
		for j := 0; j < skillCount; j++ {
			char.Skills[j] = Skill{
				ID:       gofakeit.UUID(),
				Name:     gofakeit.Word(),
				Level:    gofakeit.Number(1, 10),
				Cooldown: gofakeit.Number(0, 300),
			}
		}

		// Generate quests
		questCount := gofakeit.Number(2, 8)
		char.Quests = make([]Quest, questCount)
		for j := 0; j < questCount; j++ {
			char.Quests[j] = Quest{
				ID:       gofakeit.UUID(),
				Name:     gofakeit.Sentence(3),
				Progress: gofakeit.Number(0, 100),
				Status:   gofakeit.RandomString([]string{"active", "completed", "failed", "abandoned"}),
			}
		}

		storage.Characters[i] = char
	}

	return storage
}

// 표준 JSON 직렬화 성능 측정 (20개 캐릭터)
// 실행: go test -run=^$ -bench=BenchmarkStandardJSON20Characters -benchmem
func BenchmarkStandardJSON20Characters(b *testing.B) {
	data := generateDummyCharacters(20)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jsonData, _ := json.Marshal(data)
		_ = jsonData
	}
}

// Avro JSON 직렬화 성능 측정 (20개 캐릭터) - 스키마 검증 포함
// 실행: go test -run=^$ -bench=BenchmarkAvroJSON20Characters -benchmem
func BenchmarkAvroJSON20Characters(b *testing.B) {
	data := generateDummyCharacters(20)
	codec, _ := goavro.NewCodec(userCharacterSchema)

	// Convert to map for Avro
	dataMap := map[string]interface{}{
		"user_id": data.UserID,
		"characters": func() []interface{} {
			chars := make([]interface{}, len(data.Characters))
			for i, char := range data.Characters {
				chars[i] = map[string]interface{}{
					"id":         char.ID,
					"name":       char.Name,
					"level":      char.Level,
					"experience": char.Experience,
					"stats": map[string]interface{}{
						"health":   char.Stats.Health,
						"mana":     char.Stats.Mana,
						"strength": char.Stats.Strength,
						"defense":  char.Stats.Defense,
						"agility":  char.Stats.Agility,
						"magic":    char.Stats.Magic,
					},
					"inventory": func() []interface{} {
						items := make([]interface{}, len(char.Inventory))
						for j, item := range char.Inventory {
							items[j] = map[string]interface{}{
								"id":       item.ID,
								"name":     item.Name,
								"type":     item.Type,
								"quantity": item.Quantity,
								"rarity":   item.Rarity,
							}
						}
						return items
					}(),
					"skills": func() []interface{} {
						skills := make([]interface{}, len(char.Skills))
						for j, skill := range char.Skills {
							skills[j] = map[string]interface{}{
								"id":       skill.ID,
								"name":     skill.Name,
								"level":    skill.Level,
								"cooldown": skill.Cooldown,
							}
						}
						return skills
					}(),
					"equipment": map[string]interface{}{
						"weapon":    char.Equipment.Weapon,
						"armor":     char.Equipment.Armor,
						"accessory": char.Equipment.Accessory,
					},
					"quests": func() []interface{} {
						quests := make([]interface{}, len(char.Quests))
						for j, quest := range char.Quests {
							quests[j] = map[string]interface{}{
								"id":       quest.ID,
								"name":     quest.Name,
								"progress": quest.Progress,
								"status":   quest.Status,
							}
						}
						return quests
					}(),
					"metadata": map[string]interface{}{
						"created_at":    char.Metadata.CreatedAt,
						"last_modified": char.Metadata.LastModified,
						"play_time":     char.Metadata.PlayTime,
					},
				}
			}
			return chars
		}(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jsonData, _ := codec.TextualFromNative(nil, dataMap)
		_ = jsonData
	}
}

// Avro Binary 직렬화 성능 측정 (20개 캐릭터) - 최고 압축률
// 실행: go test -run=^$ -bench=BenchmarkAvroBinary20Characters -benchmem
func BenchmarkAvroBinary20Characters(b *testing.B) {
	data := generateDummyCharacters(20)
	codec, _ := goavro.NewCodec(userCharacterSchema)

	// Convert to map for Avro
	dataMap := map[string]interface{}{
		"user_id": data.UserID,
		"characters": func() []interface{} {
			chars := make([]interface{}, len(data.Characters))
			for i, char := range data.Characters {
				chars[i] = map[string]interface{}{
					"id":         char.ID,
					"name":       char.Name,
					"level":      char.Level,
					"experience": char.Experience,
					"stats": map[string]interface{}{
						"health":   char.Stats.Health,
						"mana":     char.Stats.Mana,
						"strength": char.Stats.Strength,
						"defense":  char.Stats.Defense,
						"agility":  char.Stats.Agility,
						"magic":    char.Stats.Magic,
					},
					"inventory": func() []interface{} {
						items := make([]interface{}, len(char.Inventory))
						for j, item := range char.Inventory {
							items[j] = map[string]interface{}{
								"id":       item.ID,
								"name":     item.Name,
								"type":     item.Type,
								"quantity": item.Quantity,
								"rarity":   item.Rarity,
							}
						}
						return items
					}(),
					"skills": func() []interface{} {
						skills := make([]interface{}, len(char.Skills))
						for j, skill := range char.Skills {
							skills[j] = map[string]interface{}{
								"id":       skill.ID,
								"name":     skill.Name,
								"level":    skill.Level,
								"cooldown": skill.Cooldown,
							}
						}
						return skills
					}(),
					"equipment": map[string]interface{}{
						"weapon":    char.Equipment.Weapon,
						"armor":     char.Equipment.Armor,
						"accessory": char.Equipment.Accessory,
					},
					"quests": func() []interface{} {
						quests := make([]interface{}, len(char.Quests))
						for j, quest := range char.Quests {
							quests[j] = map[string]interface{}{
								"id":       quest.ID,
								"name":     quest.Name,
								"progress": quest.Progress,
								"status":   quest.Status,
							}
						}
						return quests
					}(),
					"metadata": map[string]interface{}{
						"created_at":    char.Metadata.CreatedAt,
						"last_modified": char.Metadata.LastModified,
						"play_time":     char.Metadata.PlayTime,
					},
				}
			}
			return chars
		}(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binaryData, _ := codec.BinaryFromNative(nil, dataMap)
		_ = binaryData
	}
}

// 최적화된 JSON 직렬화 성능 측정 (5개 캐릭터) - 필드명 중복 제거
// 실행: go test -run=^$ -bench=BenchmarkOptimizedJSON5Characters -benchmem
func BenchmarkOptimizedJSON5Characters(b *testing.B) {
	benchmarkOptimizedJSON(b, 5)
}

// 최적화된 JSON 직렬화 성능 측정 (10개 캐릭터) - 필드명 중복 제거
// 실행: go test -run=^$ -bench=BenchmarkOptimizedJSON10Characters -benchmem
func BenchmarkOptimizedJSON10Characters(b *testing.B) {
	benchmarkOptimizedJSON(b, 10)
}

// 최적화된 JSON 직렬화 성능 측정 (20개 캐릭터) - 필드명 중복 제거
// 실행: go test -run=^$ -bench=BenchmarkOptimizedJSON20Characters -benchmem
func BenchmarkOptimizedJSON20Characters(b *testing.B) {
	benchmarkOptimizedJSON(b, 20)
}

// 최적화된 JSON 직렬화 성능 측정 (50개 캐릭터) - 필드명 중복 제거
// 실행: go test -run=^$ -bench=BenchmarkOptimizedJSON50Characters -benchmem
func BenchmarkOptimizedJSON50Characters(b *testing.B) {
	benchmarkOptimizedJSON(b, 50)
}

// 공통 최적화된 JSON 벤치마크 함수 - 배열 형태로 필드명 중복 제거
func benchmarkOptimizedJSON(b *testing.B, charCount int) {
	data := generateDummyCharacters(charCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create optimized format
		optimized := map[string]interface{}{
			"user_id":          data.UserID,
			"character_fields": []string{"id", "name", "level", "experience"},
			"stats_fields":     []string{"health", "mana", "strength", "defense", "agility", "magic"},
			"item_fields":      []string{"id", "name", "type", "quantity", "rarity"},
			"skill_fields":     []string{"id", "name", "level", "cooldown"},
			"equipment_fields": []string{"weapon", "armor", "accessory"},
			"quest_fields":     []string{"id", "name", "progress", "status"},
			"metadata_fields":  []string{"created_at", "last_modified", "play_time"},
			"characters": func() [][]interface{} {
				chars := make([][]interface{}, len(data.Characters))
				for i, char := range data.Characters {
					chars[i] = []interface{}{
						char.ID, char.Name, char.Level, char.Experience,
						[]interface{}{char.Stats.Health, char.Stats.Mana, char.Stats.Strength, char.Stats.Defense, char.Stats.Agility, char.Stats.Magic},
						func() [][]interface{} {
							items := make([][]interface{}, len(char.Inventory))
							for j, item := range char.Inventory {
								items[j] = []interface{}{item.ID, item.Name, item.Type, item.Quantity, item.Rarity}
							}
							return items
						}(),
						func() [][]interface{} {
							skills := make([][]interface{}, len(char.Skills))
							for j, skill := range char.Skills {
								skills[j] = []interface{}{skill.ID, skill.Name, skill.Level, skill.Cooldown}
							}
							return skills
						}(),
						[]interface{}{char.Equipment.Weapon, char.Equipment.Armor, char.Equipment.Accessory},
						func() [][]interface{} {
							quests := make([][]interface{}, len(char.Quests))
							for j, quest := range char.Quests {
								quests[j] = []interface{}{quest.ID, quest.Name, quest.Progress, quest.Status}
							}
							return quests
						}(),
						[]interface{}{char.Metadata.CreatedAt, char.Metadata.LastModified, char.Metadata.PlayTime},
					}
				}
				return chars
			}(),
		}

		jsonData, _ := json.Marshal(optimized)
		_ = jsonData
	}
}
