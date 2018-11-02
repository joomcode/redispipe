package redis

import (
	"fmt"
	"io"
	"strings"
	"sync"
)

var (
	// ErrOpts - options are wrong
	ErrOpts = NewErrorKind("ErrOpts", "wrong options")
	// ErrContextIsNil - context is not passed to constructor
	ErrContextIsNil = ErrOpts.SubKind("ErrContextIsNil", "context is not set")
	// ErrNoAddressProvided - no address is given to constructor
	ErrNoAddressProvided = ErrOpts.SubKind("ErrNoAddressProvided", "no address provided")

	// ErrContextClosed - context were explicitly closed (or connection / cluster were shut down)
	ErrContextClosed = NewErrorKind("ErrContextClosed", "context or connection were closed")

	// ErrKindConnection - connection was not established at the moment request were done,
	// request is definitely not sent anywhere.
	ErrConnection = NewErrorKind("ErrConnection", "connection is not established at the moment")
	// ErrNotConnected - connection were not established at the moment
	ErrNotConnected = ErrConnection.SubKind("ErrNotConnected", "connection is not established")
	// ErrDial - could not connect.
	ErrDial = ErrConnection.SubKind("ErrDial", "could not connect")
	// ErrAuth - password didn't match
	ErrAuth = ErrConnection.SubKind("ErrAuth", "auth is not successful")
	// ErrConnSetup - other connection initialization error (including io errors)
	ErrConnSetup = ErrConnection.SubKind("ErrConnSetup", "connection setup unsuccessful")

	// ErrIO - io error: read/write error, or timeout, or connection closed while reading/writting
	// It is not known if request were processed or not
	ErrIO = NewErrorKind("ErrIO", "io error")

	// ErrRequest - request malformed. Can not serialize request, no reason to retry.
	ErrRequest = NewErrorKind("ErrRequest", "request malformed")
	// ErrArgumentType - argument is not serializable
	ErrArgumentType = ErrRequest.SubKind("ErrArgumentType", "command argument type not supported")
	// ErrBatchFormant - some other command in batch is malformed
	ErrBatchFormat = ErrRequest.SubKind("ErrBatchFormat", "one of batch command is malformed")
	// ErrNoSlotKey - no key to determine cluster slot
	ErrNoSlotKey = ErrRequest.SubKind("ErrNoSlotKey", "no key to determine slot")
	// ErrRequestCancelled - request already cancelled
	ErrRequestCancelled = ErrRequest.SubKind("ErrRequestCancelled", "request was already cancelled")

	// ErrResponse - response malformed. Redis returns unexpected response.
	ErrResponse = NewErrorKind("ErrResponse", "response malformed")
	// ErrResponseFormat - response is not valid Redis response
	ErrResponseFormat = ErrResponse.SubKind("ErrResponseFormat", "redis response is malformed")
	// ErrResponseUnexpected - response is valid redis response, but its structure/type unexpected
	ErrResponseUnexpected = ErrResponse.SubKind("ErrResponseUnexpected", "redis response is unexpected")
	// ErrHeaderlineTooLarge - header line too large
	ErrHeaderlineTooLarge = ErrResponse.SubKind("ErrHeaderlineTooLarge", "headerline too large")
	// ErrHeaderlineEmpty - header line is empty
	ErrHeaderlineEmpty = ErrResponse.SubKind("ErrHeaderlineEmpty", "headerline is empty")
	// ErrIntegerParsing - integer malformed
	ErrIntegerParsing = ErrResponse.SubKind("ErrIntegerParsing", "integer is not integer")
	// ErrNoFinalRN - no final "\r\n"
	ErrNoFinalRN = ErrResponse.SubKind("ErrNoFinalRN", "no final \r\n in response")
	// ErrUnknownHeaderType - unknown header type
	ErrUnknownHeaderType = ErrResponse.SubKind("ErrUnknownHeaderType", "header type is not known")
	// ErrPing - ping receives wrong response
	ErrPing = ErrResponse.SubKind("ErrPing", "ping response doesn't match")

	// ErrResult - just regular redis response.
	ErrResult = NewErrorKind("ErrKind", "regular redis error")
	// ErrMoved - MOVED response
	ErrMoved = ErrResult.SubKind("ErrMoved", "slot were moved")
	// ErrAsk - ASK response
	ErrAsk = ErrResult.SubKind("ErrAsk", "ask another host")
	// ErrLoading - redis didn't finish start
	ErrLoading = ErrResult.SubKind("ErrLoading", "host is loading")
	// ErrExecEmpty - EXEC returns nil (WATCH failed) (it is strange, cause we don't support WATCH)
	ErrExecEmpty = ErrResult.SubKind("ErrExecEmpty", "exec failed because of WATCH???")

	// ErrCluster - some cluster related errors.
	ErrCluster = NewErrorKind("ErrCluster", "cluster related error")
	// ErrClusterSlots - fetching slots configuration failed
	ErrClusterSlots = ErrCluster.SubKind("ErrClusterSlots", "could not retrieve slots from redis")
	// ErrAddressNotResolved - address could not be resolved
	// Cluster resolves named hosts specified as start points. If this resolution fails, this error returned.
	ErrAddressNotResolved = ErrCluster.SubKind("ErrAddressNotResolved", "address is not resolved.")
	// ErrClusterConfigEmpty - no addresses found in config.
	ErrClusterConfigEmpty = ErrCluster.SubKind("ErrClusterConfigEmpty", "cluster configuration is emptry.")
)

// ErrorKind is a kind of error
type ErrorKind struct{ *errorKind }

type errorKind struct {
	parent   ErrorKind
	name     string
	message  string
	subtypes map[string]ErrorKind
	mtx      sync.Mutex
}

var rootKind = ErrorKind{&errorKind{subtypes: map[string]ErrorKind{}}}

// NewErrorKind registers new error kind (or returns already existed kind with the same name)
func NewErrorKind(name string, defaultMessage string) ErrorKind {
	return rootKind.SubKind(name, defaultMessage)
}

// SubKind creates new kind descendant to current.
func (e ErrorKind) SubKind(name string, defaultMessage string) ErrorKind {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	kind, ok := e.subtypes[name]
	if !ok {
		kind = ErrorKind{&errorKind{
			parent:   e,
			name:     name,
			message:  defaultMessage,
			subtypes: map[string]ErrorKind{},
		}}
		e.subtypes[name] = kind
	} else if kind.message != defaultMessage {
		panic("attempt to redefine error kind with different message")
	}
	return kind
}

// String implements fmt.Stringer
func (e ErrorKind) String() string {
	if e.parent != rootKind {
		return e.parent.String() + "/" + e.name
	}
	return e.name
}

// GoString implements fmt.GoStringer
func (e ErrorKind) GoString() string {
	return e.String()
}

// Message returns default message.
func (e ErrorKind) Message() string {
	if e.message != "" {
		return e.message
	}
	return e.parent.message
}

// Name returns only type's name
func (e ErrorKind) Name() string {
	return e.name
}

// FullName returns full hierarchy name
// It is alias for String.
func (e ErrorKind) FullName() string {
	return e.String()
}

// New returns error with this kind
func (e ErrorKind) New() *Error {
	return &Error{kind: e}
}

// NewMsg returns error with this kind and message
func (e ErrorKind) NewMsg(msg string) *Error {
	return (&Error{kind: e}).WithMsg(msg)
}

// NewErr returns error with this kind that wraps another common error
func (e ErrorKind) NewWrap(err error) *Error {
	return (&Error{kind: e}).Wrap(err)
}

// KindOf retruns if this error is equal to or is descendant of other kind.
func (e ErrorKind) KindOf(o ErrorKind) bool {
loop:
	if e == o {
		return true
	}
	if e.parent == rootKind {
		return false
	}
	e = e.parent
	goto loop
}

// Error is an error returned by connector
type Error struct {
	kind ErrorKind
	*kv
}

// Kind returns kind of error.
func (e *Error) Kind() ErrorKind {
	return e.kind
}

// KindOf returns if this error is kind of kind.
// It is equal to e.Kind().KindOf(k)
func (e *Error) KindOf(k ErrorKind) bool {
	return e.kind.KindOf(k)
}

// WithMsg returns copy of error with new message.
func (copy Error) WithMsg(msg string) *Error {
	return copy.With("message", msg)
}

// Wrap returns copy of error with wrapped cause.
func (copy Error) Wrap(err error) *Error {
	return copy.With("cause", err)
}

// With returns copy of error with name-value pair attached
func (copy Error) With(name string, value interface{}) *Error {
	copy.kv = &kv{name: name, value: value, next: copy.kv}
	return &copy
}

// HardError returns true if error is not nil and it is not kind of ErrKindResult (ie not error returned by redis).
func HardError(e *Error) bool {
	return e != nil && !e.KindOf(ErrResult)
}

// Error implements error.Error.
func (e Error) Error() string {
	typ := e.Kind().String()
	msg := e.Msg()
	rest := e.restAsString()
	if rest != "" {
		return fmt.Sprintf("%s (%s %s)", msg, typ, rest)
	} else {
		return fmt.Sprintf("%s (%s)", msg, typ)
	}
}

// Format implements fmt.Formatter.Format.
func (e Error) Format(f fmt.State, c rune) {
	io.WriteString(f, e.Error())
}

// Msg returns message associated with error (value, associated with "message" key).
// If message were not set explicit, but cause were set, then cause.Error() is taken.
// If cause is not set, then default message for code is taken.
// Otherwise "generic"
func (e Error) Msg() string {
	var msg string
	var ok bool
	if msgo := e.Get("message"); msgo != nil {
		switch m := msgo.(type) {
		case string:
			msg = m
		case fmt.Stringer:
			msg = m.String()
		case fmt.GoStringer:
			msg = m.GoString()
		case error:
			msg = m.Error()
		default:
			msg = fmt.Sprint(m)
		}
		ok = true
	}
	if !ok {
		if err := e.Cause(); err != nil {
			msg = err.Error()
			ok = true
		}
	}
	if !ok {
		msg = e.Kind().Message()
		if msg == "" {
			msg = "generic"
		}
	}
	return msg
}

// Cause returns wrapped error (in fact, value associated with "cause" key).
func (e Error) Cause() error {
	if ierr := e.Get("cause"); ierr != nil {
		if err, ok := ierr.(error); ok {
			return err
		}
	}
	return nil
}

func (e Error) restAsString() string {
	parts := []string{}
	kv := e.kv
	for kv != nil {
		if kv.name != "message" && kv.name != "cause" {
			parts = append(parts, fmt.Sprintf("%s: %v", kv.name, kv.value))
		}
		kv = kv.next
	}
	if len(parts) > 0 {
		return "{" + strings.Join(parts, ", ") + "}"
	} else {
		return ""
	}
}

// ToMap returns information assiciated with error as a map.
func (e Error) ToMap() map[string]interface{} {
	res := map[string]interface{}{
		"kind": e.Kind(),
	}
	kv := e.kv
	for kv != nil {
		res[kv.name] = kv.value
		kv = kv.next
	}
	return res
}

type kv struct {
	name  string
	value interface{}
	next  *kv
}

// Get searches corresponding key.
func (kv *kv) Get(name string) interface{} {
	for kv != nil {
		if kv.name == name {
			return kv.value
		}
		kv = kv.next
	}
	return nil
}
