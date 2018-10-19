package redis

import (
	"fmt"
	"io"
	"strings"
)

type ErrorKind uint32
type ErrorCode uint32

type Error struct {
	Kind ErrorKind
	Code ErrorCode
	*kv
}

const (
	// options are wrong
	ErrKindOpts ErrorKind = iota + 1
	// context explicitely closed
	ErrKindContext
	// Connection was not established at the moment request were done,
	// Request is definitely not sent anywhere.
	ErrKindConnection
	// io error: read/write error, or timeout, or connection closed while reading/writting
	// It is not known if request were processed or not
	ErrKindIO
	// request malformed
	// Can not serialize request, no reason to retry.
	ErrKindRequest
	// response malformed
	// Redis returns unexpected response
	ErrKindResponse
	// cluster configuration inconsistent
	ErrKindCluster
	// Just regular redis error response
	ErrKindResult
)

var kindName = map[ErrorKind]string{
	ErrKindOpts:       "ErrKindOpts",
	ErrKindContext:    "ErrKindContext",
	ErrKindConnection: "ErrKindConnection",
	ErrKindIO:         "ErrKindIO",
	ErrKindRequest:    "ErrKindRequest",
	ErrKindResponse:   "ErrKindResponse",
	ErrKindCluster:    "ErrKindCluster",
	ErrKindResult:     "ErrKindResult",
}

func (k ErrorKind) String() string {
	if s, ok := kindName[k]; ok {
		return s
	}
	return fmt.Sprintf("ErrKindUnknown%d", k)
}

func (k ErrorKind) GoString() string {
	return k.String()
}

const (
	// context is not passed to contructor
	// (ErrKindOpts)		0x1
	ErrContextIsNil ErrorCode = iota + 1
	// (ErrKindOpts)		0x2
	ErrNoAddressProvided
	// context were explicitely closed (connection or cluster shut down)
	// (ErrKindContext)		0x3
	ErrContextClosed
	// connection were not established at the moment
	// (ErrKindConnection)	0x4
	ErrNotConnected
	// connection establishing not successful
	// (ErrKindConnection)	0x5
	ErrDial
	// password didn't match
	// (ErrKindConnection)	0x6
	ErrAuth
	// other connection initializing error
	// (ErrKindConnection)	0x7
	ErrConnSetup
	// connection were closed, or other read-write error
	// (ErrKindIO or ErrKindConnection) 0x8
	ErrIO
	// Argument is not serializable
	// (ErrKindRequest)		0x9
	ErrArgumentType
	// Some other command in batch is malformed
	// (ErrKindRequest)		0xa
	ErrBatchFormat
	// Response is not valid Redis response
	// (ErrKindResponse)	0xb
	ErrResponseFormat
	// Response is valid redis response, but its structure/type unexpected
	// (ErrKindResponse)	0xc
	ErrResponseUnexpected
	// Header line too large
	// (ErrKindResponse)	0xd
	ErrHeaderlineTooLarge
	// Header line is empty
	// (ErrKindResponse)	0xe
	ErrHeaderlineEmpty
	// Integer malformed
	// (ErrKindResponse)	0xf
	ErrIntegerParsing
	// No final "\r\n"
	// (ErrKindResponse)	0x10
	ErrNoFinalRN
	// Unknown header type
	// (ErrKindResponse)	0x11
	ErrUnknownHeaderType
	// Ping receives wrong response
	// (ErrKindResponse)	0x12
	ErrPing
	// Just regular redis response
	// (ErrKindResult)		0x13
	ErrResult
	// Special case for MOVED
	// (ErrKindResult)		0x14
	ErrMoved
	// Special case for ASK
	// (ErrKindResult)		0x15
	ErrAsk
	// Special case for LOADING
	// (ErrKindResult)		0x16
	ErrLoading
	// No key to determine cluster slot
	// (ErrKindRequest)		0x17
	ErrNoSlotKey
	// Fething slots failed
	// (ErrKindCluster)		0x18
	ErrClusterSlots
	// EXEC returns nil (WATCH failed) (it is strange, cause we don't support WATCH)
	// (ErrKindResult)		0x19
	ErrExecEmpty
	// No addresses found in config
	// (ErrKindCluster)		0x1a
	ErrClusterConfigEmpty
	// Request already cancelled
	// (ErrKindRequest)		0x1b
	ErrRequestCancelled
	// Address could not be resolved
	// (ErrAddressNotResolved) 0x1c
	ErrAddressNotResolved
)

var codeName = map[ErrorCode]string{
	ErrContextIsNil:   "ErrContextIsNil",
	ErrContextClosed:  "ErrContextClosed",
	ErrNotConnected:   "ErrNotConnected",
	ErrDial:           "ErrDial",
	ErrAuth:           "ErrAuth",
	ErrConnSetup:      "ErrConnSetup",
	ErrIO:             "ErrIO",
	ErrArgumentType:   "ErrArgumentType",
	ErrBatchFormat:    "ErrBatchFormat",
	ErrResponseFormat: "ErrResponseFormat",
	ErrPing:           "ErrPing",
	ErrResult:         "ErrResult",
	ErrMoved:          "ErrMoved",
	ErrAsk:            "ErrAsk",
	ErrLoading:        "ErrLoading",
	ErrNoSlotKey:      "ErrNoSlotKey",
	ErrClusterSlots:   "ErrClusterSlots",
	ErrExecEmpty:      "ErrExecEmpty",

	ErrRequestCancelled:   "ErrRequestCancelled",
	ErrClusterConfigEmpty: "ErrClusterConfigEmpty",
	ErrResponseUnexpected: "ErrResponseUnexpected",
	ErrHeaderlineTooLarge: "ErrHeaderlineTooLarge",
	ErrHeaderlineEmpty:    "ErrHeaderlineEmpty",
	ErrIntegerParsing:     "ErrIntegerParsing",
	ErrNoFinalRN:          "ErrNoFinalRN",
	ErrUnknownHeaderType:  "ErrUnknownHeaderType",
}

func (c ErrorCode) String() string {
	if s, ok := codeName[c]; ok {
		return s
	}
	return fmt.Sprintf("ErrUnknown%d", c)
}

func (c ErrorCode) GoString() string {
	return c.String()
}

var defMessage = map[ErrorCode]string{
	ErrContextIsNil:   "context is not set",
	ErrContextClosed:  "context is closed",
	ErrNotConnected:   "connection is not established",
	ErrDial:           "could not connect",
	ErrAuth:           "auth is not successful",
	ErrIO:             "io error",
	ErrConnSetup:      "connection setup unsuccessful",
	ErrArgumentType:   "command argument type not supported",
	ErrBatchFormat:    "one of batch command is malformed",
	ErrResponseFormat: "redis response is malformed",
	ErrPing:           "ping response doesn't match",
	ErrMoved:          "slot moved",
	ErrAsk:            "ask another",
	ErrLoading:        "host is loading",
	ErrNoSlotKey:      "no key to determine slot",
	ErrClusterSlots:   "could not retrieve slots from redis",
	ErrExecEmpty:      "exec failed because of WATCH???",

	ErrRequestCancelled:   "request was already cancelled",
	ErrClusterConfigEmpty: "cluster configuration is empty",
	ErrResponseUnexpected: "redis response is unexpected",
	ErrHeaderlineTooLarge: "headerline too large",
	ErrHeaderlineEmpty:    "headerline is empty",
	ErrIntegerParsing:     "integer is not integer",
	ErrNoFinalRN:          "no final \r\n in response",
	ErrUnknownHeaderType:  "header type is not known",

	//ErrResult:         "",
}

func NewErr(kind ErrorKind, code ErrorCode) *Error {
	return &Error{Kind: kind, Code: code}
}

func NewErrMsg(kind ErrorKind, code ErrorCode, msg string) *Error {
	return Error{Kind: kind, Code: code}.With("message", msg)
}

func NewErrWrap(kind ErrorKind, code ErrorCode, err error) *Error {
	return Error{Kind: kind, Code: code}.With("cause", err)
}

func (copy Error) WithMsg(msg string) *Error {
	return copy.With("message", msg)
}

func (copy Error) Wrap(err error) *Error {
	return copy.With("cause", err)
}

func (copy Error) With(name string, value interface{}) *Error {
	// This could be called from many places concurrently, so need to
	// copy error
	copy.kv = &kv{name: name, value: value, next: copy.kv}
	return &copy
}

func (e *Error) HardError() bool {
	return e != nil && e.Kind != ErrKindResult
}

func (e Error) Error() string {
	typ := e.Code.String()
	if typ == "" {
		typ = fmt.Sprintf("ErrUnknown%d", e.Code)
	}
	msg := e.Msg()
	rest := e.restAsString()
	if rest != "" {
		return fmt.Sprintf("%s (%s %s)", msg, typ, rest)
	} else {
		return fmt.Sprintf("%s (%s)", msg, typ)
	}
}

func (e Error) Format(f fmt.State, c rune) {
	io.WriteString(f, e.Error())
}

func (e Error) Msg() string {
	msg, ok := e.Get("message").(string)
	if !ok {
		if err := e.Cause(); err != nil {
			msg = err.Error()
			ok = true
		}
	}
	if !ok {
		msg = defMessage[e.Code]
		if msg == "" {
			msg = "generic "
		}
	}
	return msg
}

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

func (e Error) ToMap() map[string]interface{} {
	res := map[string]interface{}{
		"kind": e.Kind,
		"code": e.Code,
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

func (kv *kv) Get(name string) interface{} {
	for kv != nil {
		if kv.name == name {
			return kv.value
		}
		kv = kv.next
	}
	return nil
}
