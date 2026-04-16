package v2

// UserRegistered is version 2 with additional fields for enhanced tracking.
// Breaking change from v1: Added Country (required) and RegisteredAt fields.
type UserRegistered struct {
	Email        string `json:"email"`
	Name         string `json:"name"`
	Country      string `json:"country"`       // New in v2
	RegisteredAt int64  `json:"registered_at"` // New in v2: Unix timestamp
}
