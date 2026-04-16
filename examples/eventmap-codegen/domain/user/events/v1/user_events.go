package v1

// UserRegistered is emitted when a new user registers in the system.
type UserRegistered struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// UserEmailChanged is emitted when a user changes their email address.
type UserEmailChanged struct {
	OldEmail string `json:"old_email"`
	NewEmail string `json:"new_email"`
}

// UserDeleted is emitted when a user account is deleted.
type UserDeleted struct {
	Reason string `json:"reason"`
}
