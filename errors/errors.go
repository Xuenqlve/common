package errors

import perrors "github.com/pingcap/errors"

const (
	ErrCodePanic            = 1000
	ErrCodeMessagePoint     = 2000
	ErrCodeMessageTransform = 3000
)

type DtsError struct {
	Code uint16
	error
}

func NewDtsError(code uint16, err error) error {
	return &DtsError{
		Code:  code,
		error: err,
	}
}

func NewDtsErrorMessage(code uint16, message string) error {
	return &DtsError{
		Code:  code,
		error: perrors.New(message),
	}
}

var (
	ErrMessageInPointNil = NewDtsErrorMessage(ErrCodeMessagePoint, "message point nil")
)
