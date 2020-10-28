package dyno

import "github.com/go-playground/validator/v10"

// use a single instance of Validate, it caches struct info
var validate *validator.Validate

// lazy loader that grabs the validator instance
func Validator() *validator.Validate {
	if validate == nil {
		validate = validator.New()
	}
	return validate
}
