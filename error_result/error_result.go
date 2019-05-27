package error_result


type ErrorResult struct {
	error
	Code       int `json:"code"`
	Message string `json:"message"`
}

func (err ErrorResult) Error() string {
	return err.Message
}
