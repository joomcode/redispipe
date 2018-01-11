package resp

import "errors"

var ErrHeaderlineTooLarge = errors.New("resp value header too large")
var ErrIntegerParsing = errors.New("resp integer malformed")
var ErrNoFinalRN = errors.New("resp no final \\r\\n found for value")
var ErrUnknownHeaderType = errors.New("resp unknown header type")

type IOError struct{ error }

type ResponseError struct{ Msg string }

func (r ResponseError) Error() string {
	return r.Msg
}
