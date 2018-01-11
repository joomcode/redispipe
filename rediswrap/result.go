package rediswrap

type IResult interface {
	Value() interface{}
	Error() error
	AnyError() error
}

type Result struct {
	val interface{}
	err error
}

func (r Result) Value() interface{} {
	return r.val
}

func (r Result) Error() error {
	return r.err
}

func (r Result) AnyError() error {
	if r.err != nil {
		return r.err
	} else if err, ok := r.val.(error); ok {
		return err
	}
	return nil
}
