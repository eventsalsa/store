package v2

// UserRegistered is version 2 with additional fields.
type UserRegistered struct {
	Email     string `json:"email"`
	Name      string `json:"name"`
	Country   string `json:"country"`
	Timestamp int64  `json:"timestamp"`
}
