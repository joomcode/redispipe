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

	// ErrConnection - connection was not established at the moment request were done,
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
	// ErrBatchFormat - some other command in batch is malformed
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
	ErrResult = NewErrorKind("ErrResult", "regular redis error")
	// ErrMoved - MOVED response
	ErrMoved = ErrResult.SubKind("ErrMoved", "slot were moved")
	// ErrAsk - ASK response
	ErrAsk = ErrResult.SubKind("ErrAsk", "ask another host")
	// ErrLoading - redis didn't finish start
	ErrLoading = ErrResult.SubKind("ErrLoading", "host is loading")
	// ErrExecEmpty - EXEC returns nil (WATCH failed) (it is strange, cause we don't support WATCH)
	ErrExecEmpty = ErrResult.SubKind("ErrExecEmpty", "exec failed because of WATCH???")
)

var (
	// EKMessage - key to store message associated with error.
	// Note: you'd better use `Msg()` method.
	EKMessage = NewErrorKey("message")
	// EKCause - key to store wrapped error.
	// There is Cause() convenient method to get it.
	EKCause = NewErrorKey("cause")
	// EKLine - set by response parser for unrecognized header lines.
	EKLine = NewErrorKey("line")
	// EKMovedTo - set by response parser for MOVED and ASK responses.
	EKMovedTo = NewErrorKey("movedto")
	// EKSlot - set by response parser for MOVED and ASK responses.
	EKSlot = NewErrorKey("slot")
	// EKVal - set by request writer and checker to argument value which could not be serialized.
	EKVal = NewErrorKey("val")
	// EKArgPos - set by request writer and checker to argument position which could not be serialized.
	EKArgPos = NewErrorKey("argpos")
	// EKRequest - request that triggered error.
	EKRequest = NewErrorKey("request")
	// EKRequests - batch requests that triggered error.
	EKRequests = NewErrorKey("requests")
	// EKResponse - unexpected response
	EKResponse = NewErrorKey("response")
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

// NewWrap returns error with this kind that wraps another common error
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

// ErrorKey is a type that could be used as a custom key for key-value pair associated with error.
// It is thin wrapper around string made for type-safety and performance.
type ErrorKey struct {
	name *string
}

var errkeys = struct {
	sync.Mutex
	keys map[string]ErrorKey
}{keys: make(map[string]ErrorKey)}

// NewErrorKey creates new key for specified name or returns existing one.
func NewErrorKey(name string) ErrorKey {
	errkeys.Lock()
	defer errkeys.Unlock()
	key, ok := errkeys.keys[name]
	if !ok {
		key = ErrorKey{&name}
		errkeys.keys[name] = key
	}
	return key
}

// String implements fmt.Stringer
func (ek ErrorKey) String() string {
	return *ek.name
}

// GoString implements fmt.GoStringer
func (ek ErrorKey) GoString() string {
	return *ek.name
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
func (e Error) WithMsg(msg string) *Error {
	return e.With(EKMessage, msg)
}

// Wrap returns copy of error with wrapped cause.
func (e Error) Wrap(err error) *Error {
	return e.With(EKCause, err)
}

// With returns copy of error with name-value pair attached
func (e Error) With(key ErrorKey, value interface{}) *Error {
	e.kv = &kv{key: key, value: value, next: e.kv}
	return &e
}

// WithNewKey returns copy of error with name-value pair attached.
// If such key were already set, then do nothing.
func (e *Error) WithNewKey(key ErrorKey, value interface{}) *Error {
	if e.Get(key) != nil {
		return e
	}
	copy := *e
	copy.kv = &kv{key: key, value: value, next: copy.kv}
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
	if rest == "" {
		return fmt.Sprintf("%s (%s)", msg, typ)
	}
	return fmt.Sprintf("%s (%s %s)", msg, typ, rest)
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
	if msgo := e.Get(EKMessage); msgo != nil {
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
	if ierr := e.Get(EKCause); ierr != nil {
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
		if kv.key != EKMessage {
			parts = append(parts, fmt.Sprintf("%s: %v", *kv.key.name, kv.value))
		}
		kv = kv.next
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// ToMap returns information assiciated with error as a map.
func (e Error) ToMap() map[string]interface{} {
	res := map[string]interface{}{
		"kind": e.Kind(),
	}
	kv := e.kv
	for kv != nil {
		res[*kv.key.name] = kv.value
		kv = kv.next
	}
	return res
}

type kv struct {
	key   ErrorKey
	value interface{}
	next  *kv
}

// Get searches corresponding key.
func (kv *kv) Get(key ErrorKey) interface{} {
	for kv != nil {
		if kv.key == key {
			return kv.value
		}
		kv = kv.next
	}
	return nil
}

// GetByName searches corresponding key by its name.
func (kv *kv) GetByName(name string) interface{} {
	for kv != nil {
		if *kv.key.name == name {
			return kv.value
		}
		kv = kv.next
	}
	return nil
}
