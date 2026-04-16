package v1

// UserRegistered is emitted when a new user registers.
type UserRegistered struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// UserEmailChanged is emitted when a user changes their email.
type UserEmailChanged struct {
	OldEmail string `json:"old_email"`
	NewEmail string `json:"new_email"`
}
