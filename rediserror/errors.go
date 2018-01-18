package rediserror

import (
	"fmt"
	"strings"
)

type Error struct {
	Kind  int
	Code  int
	Text  string
	Cause error
	Data  *KV
}

const (
	// options are wrong
	ErrKindOpts = iota + 1
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

const (
	// context is not passed to contructor
	// (ErrKindOpts)
	ErrContextIsNil = iota + 1
	// (ErrKindOpts)
	ErrNoAddressProvided
	// context were explicitely closed (connection or cluster shut down)
	// (ErrKindContext)
	ErrContextClosed
	// connection were not established at the moment
	// (ErrKindConnection)
	ErrNotConnected
	// connection establishing not successful
	// (ErrKindConnection)
	ErrDial
	// password didn't match
	// (ErrKindConnection)
	ErrAuth
	// other connection initializing error
	// (ErrKindConnection)
	ErrConnSetup
	// connection were closed, or other read-write error
	// (ErrKindIO or ErrKindConnection)
	ErrIO
	// Argument is not serializable
	// (ErrKindRequest)
	ErrArgumentType
	// Some other command in batch is malformed
	// (ErrKindRequest)
	ErrBatchFormat
	// Response is not valid Redis response
	// (ErrKindResponse)
	ErrResponseFormat
	// Header line too large
	// (ErrKindResponse)
	ErrHeaderlineTooLarge
	// Header line is empty
	// (ErrKindResponse)
	ErrHeaderlineEmpty
	// Integer malformed
	// (ErrKindResponse)
	ErrIntegerParsing
	// No final "\r\n"
	// (ErrKindResponse)
	ErrNoFinalRN
	// Unknown header type
	// (ErrKindResponse)
	ErrUnknownHeaderType
	// Ping receives wrong response
	// (ErrKindResponse)
	ErrPing
	// Just regular redis response
	// (ErrKindResult)
	ErrResult
	// Special case for MOVED
	// (ErrKindResult)
	ErrMoved
	// Special case for ASK
	// (ErrKindResult)
	ErrAsk
	// Special case for LOADING
	// (ErrKindResult)
	ErrLoading
	// No key to determine cluster slot
	// (ErrKindRequest)
	ErrNoSlotKey
	// Fething slots failed
	// (ErrKindCluster)
	ErrClusterSlots
	// EXEC returns nil (WATCH failed) (it is strange, cause we don't support WATCH)
	// (ErrKindResult)
	ErrExecEmpty
	// No addresses found in config
	// (ErrKindCluster)
	ErrClusterConfigEmpty
)

var typeName = map[int]string{
	ErrContextIsNil:   "ErrContextIsNil",
	ErrContextClosed:  "ErrContextClosed",
	ErrNotConnected:   "ErrNotConnected",
	ErrDial:           "ErrDial",
	ErrAuth:           "ErrAuth",
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

	ErrHeaderlineTooLarge: "ErrHeaderlineTooLarge",
	ErrIntegerParsing:     "ErrIntegerParsing",
	ErrNoFinalRN:          "ErrNoFinalRN",
	ErrUnknownHeaderType:  "ErrUnknownHeaderType",
}

var defMessage = map[int]string{
	ErrContextIsNil:   "context is not set",
	ErrContextClosed:  "context is closed",
	ErrNotConnected:   "connection is not established",
	ErrDial:           "could not connect",
	ErrAuth:           "auth is not successful",
	ErrIO:             "io error",
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

	ErrHeaderlineTooLarge: "headerline too large",
	ErrIntegerParsing:     "integer is not integer",
	ErrNoFinalRN:          "no final \r\n in response",
	ErrUnknownHeaderType:  "header type is not known",

	//ErrResult:         "",
}

func New(kind, code int) *Error {
	return &Error{
		Kind: kind,
		Code: code,
	}
}

func NewMsg(kind, code int, msg string) *Error {
	return &Error{
		Kind: kind,
		Code: code,
		Text: msg,
	}
}

func NewWrap(kind, code int, err error) *Error {
	return &Error{
		Kind:  kind,
		Code:  code,
		Cause: err,
	}
}

func (copy Error) With(name string, value interface{}) *Error {
	// This could be called from many places concurrently, so need to
	// copy error
	copy.Data = &KV{Name: name, Value: value, Next: copy.Data}
	return &copy
}

func (e *Error) HardError() bool {
	return e != nil && e.Kind != ErrKindResult
}

func (e Error) Error() string {
	typ := typeName[e.Code]
	if typ == "" {
		typ = fmt.Sprintf("ErrUnknown%d", e.Code)
	}
	msg := e.Text
	if msg == "" && e.Cause != nil {
		msg = e.Cause.Error()
	}
	if msg == "" {
		msg = defMessage[e.Code]
		if msg == "" {
			msg = "generic "
		}
	}
	if e.Data != nil {
		return fmt.Sprintf("%s (%s %s)", msg, typ, e.Data)
	} else {
		return fmt.Sprintf("%s (%s)", msg, typ)
	}
}

type KV struct {
	Name  string
	Value interface{}
	Next  *KV
}

func (kv *KV) Get(name string) interface{} {
	if kv == nil {
		return nil
	}
	if kv.Name == name {
		return kv.Value
	}
	return kv.Next.Get(name)
}

func (kv *KV) ToMap() map[string]interface{} {
	var kvs []*KV
	for kv != nil {
		kvs = append(kvs, kv)
		kv = kv.Next
	}
	res := make(map[string]interface{}, len(kvs))
	for i := len(kvs) - 1; i >= 0; i-- {
		res[kvs[i].Name] = kvs[i].Value
	}
	return res
}

func (kv *KV) String() string {
	parts := []string{}
	for kv != nil {
		parts = append(parts, fmt.Sprintf("%s: %v", kv.Name, kv.Value))
		kv = kv.Next
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
