// Package native is the Go binding of the core's C ABI: one function per entry
// point of `test/fixtures/abi_surface.txt`, called through cgo against the
// static `libflz_core_ffi.a`.
//
// It adds no ABI surface. What it adds is the Go side of two rules the ABI
// states: a buffer the core hands out is copied into Go memory and freed
// through flz_core_buf_free before the call returns, so no Go value ever points
// into core memory; and a failing entry's JSON error document becomes an
// *Error a caller branches on with errors.As.
package native

/*
#cgo LDFLAGS: -lflz_core_ffi
#cgo linux LDFLAGS: -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc
#cgo darwin LDFLAGS: -framework CoreFoundation -framework Security -liconv -lSystem -lc -lm

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

typedef struct FlzContext FlzContext;

uint32_t flz_core_schema_version(void);
FlzContext *flz_core_context_new(uint32_t shim_schema_version);
void flz_core_context_free(FlzContext *ctx);
const char *flz_core_init_error(void);
const char *flz_core_last_error(const FlzContext *ctx);
const char *flz_core_error_document(const char *code, const char *details_json);
void flz_core_buf_free(uint8_t *buf, size_t len);
void flz_core_set_log_callback(void (*callback)(int32_t level, const char *message));
int32_t flz_core_take_notices(uint8_t **out, size_t *out_len);
int32_t flz_core_declare_class(FlzContext *ctx, const uint8_t *json, size_t json_len);
int32_t flz_core_undeclare_class(FlzContext *ctx, const char *factory_id);
int32_t flz_core_check_class(FlzContext *ctx, const uint8_t *json, size_t json_len, uint8_t **buf_out, size_t *len_out);
int32_t flz_core_start(FlzContext *ctx, const char *ns);
int32_t flz_core_start_with(FlzContext *ctx, const char *ns, const uint8_t *settings, size_t settings_len);
int32_t flz_core_local_start(FlzContext *ctx, const char *factory_id, const char *relay_id);
int32_t flz_core_local_on_sink(FlzContext *ctx, const char *channel);
int32_t flz_core_local_feed(FlzContext *ctx, const char *channel, const uint8_t *payload, size_t len);
int32_t flz_core_local_next_output(FlzContext *ctx, uint64_t timeout_ms, uint8_t **buf_out, size_t *len_out);
int32_t flz_core_local_run_until_idle(FlzContext *ctx, uint64_t timeout_ms);
int32_t flz_core_local_stop(FlzContext *ctx);
int32_t flz_core_poll(FlzContext *ctx, uint64_t timeout_ms, uint8_t **buf_out, size_t *len_out);
int32_t flz_core_pool_open(FlzContext *ctx, uint32_t default_base, uint32_t *policy_out);
int32_t flz_core_pool_supervise(FlzContext *ctx);
int32_t flz_core_pool_left(FlzContext *ctx);
int32_t flz_core_complete(FlzContext *ctx, const uint8_t *buf, size_t len);
int32_t flz_core_abandon(FlzContext *ctx, uint64_t token);
void flz_core_shutdown(FlzContext *ctx);
void flz_core_watch_stdin_eof(void);
int32_t flz_core_host_owns_signals(void);
void flz_core_terminate(const char *reason) __attribute__((noreturn));
int32_t flz_core_spawn_request(FlzContext *ctx, uint64_t token, const char *factory_id, const char *id, const uint8_t *payload, size_t payload_len, const char *if_running, int32_t replay, int64_t offset, uint64_t *handle_out);
int32_t flz_core_spawn_result(FlzContext *ctx, uint64_t handle, uint64_t timeout_ms, uint8_t **out, size_t *out_len);
int32_t flz_core_part(FlzContext *ctx, uint64_t token, uint32_t item, const uint8_t *payload, size_t payload_len);
int32_t flz_core_effect_begin(FlzContext *ctx, uint64_t token, uint32_t item, const char *name, uint8_t **out, size_t *out_len);
int32_t flz_core_effect_end(FlzContext *ctx, uint64_t token, uint32_t item, const uint8_t *payload, size_t payload_len, uint8_t **out, size_t *out_len);
int32_t flz_core_effect_release(FlzContext *ctx, uint64_t token, uint32_t item);
int32_t flz_core_admin_call(FlzContext *ctx, const char *verb, const char *args, uint8_t **result_out, size_t *result_len_out);
int32_t flz_core_send_message(FlzContext *ctx, const char *relay_id, const char *channel, const uint8_t *message, size_t len, const char *msg_id, const char *key, int32_t local, uint8_t **result_out, size_t *result_len_out);
int32_t flz_core_relays(FlzContext *ctx, uint8_t **buf_out, size_t *len_out);
uint64_t flz_core_monotonic_nanos(void);
int32_t flz_core_knob(const char *kind, const char *raw, int64_t *out);
int32_t flz_core_response_decode(const uint8_t *bytes, size_t len, uint8_t *out, size_t *parts);
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"unsafe"
)

// SchemaVersion is the activation schema this package encodes and decodes,
// handed to flz_core_context_new so a core of another version is refused before
// anything connects. It moves with `crates/flz_core/src/schema.rs`.
const SchemaVersion = 8

// The statuses the ABI answers.
const (
	Err              = -1
	PollTimeout      = 0
	PollActivation   = 1
	PollShuttingDown = 2
	PoolIdle         = 0
	PoolSpawnBase    = 1
	PoolStop         = 2
	PoolSpawnGrown   = 3
	EffectRun        = 0
	EffectKept       = 1
	EffectUnrecorded = 2
	SpawnPending     = 1
	WaitCoreBound    = ^uint64(0)
)

// Error is a refusal in the catalogue's shape: what the core, the controller or
// the SDK itself refused, with the code a program branches on.
type Error struct {
	Code    string         `json:"code"`
	Class   string         `json:"class"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
	Hint    string         `json:"hint,omitempty"`
}

func (e *Error) Error() string {
	if e.Hint == "" {
		return fmt.Sprintf("%s (%s)", e.Message, e.Code)
	}
	return fmt.Sprintf("%s (%s); try: %s", e.Message, e.Code, e.Hint)
}

// FromDocument reads a JSON error document, `{"error": {...}}`.
func FromDocument(document []byte) *Error {
	var wrapped struct {
		Error *Error `json:"error"`
	}
	if err := json.Unmarshal(document, &wrapped); err != nil || wrapped.Error == nil {
		return &Error{Code: "runtime.internal", Class: "internal", Message: string(document)}
	}
	return wrapped.Error
}

// onThread runs one entry and, when it fails, reads its error document with
// read, both on one OS thread: the core keeps a failure's document per thread,
// and the goroutine stays on its thread, with no other run there, until the
// document is copied.
func onThread(entry func() bool, read func() *C.char) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if entry() {
		return nil
	}
	document := read()
	if document == nil {
		return errors.New("flezha: the core refused a call and recorded no reason")
	}
	return FromDocument([]byte(C.GoString(document)))
}

// contextFree runs an entry that reports through flz_core_init_error.
func contextFree(entry func() bool) error {
	return onThread(entry, func() *C.char { return C.flz_core_init_error() })
}

// Refusal renders the catalogue's error for code with its details filled, a
// refusal the SDK makes itself that reads as the core's own.
func Refusal(code string, details map[string]any) *Error {
	if details == nil {
		details = map[string]any{}
	}
	encoded, err := json.Marshal(details)
	if err != nil {
		return &Error{Code: code, Class: "internal", Message: err.Error()}
	}
	cCode, cDetails := C.CString(code), C.CString(string(encoded))
	defer C.free(unsafe.Pointer(cCode))
	defer C.free(unsafe.Pointer(cDetails))
	// The rendered document is borrowed until the next call on this thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	document := C.flz_core_error_document(cCode, cDetails)
	if document == nil {
		return &Error{Code: code, Class: "internal", Message: "the core rendered no error document for " + code}
	}
	return FromDocument([]byte(C.GoString(document)))
}

// CoreSchemaVersion is the schema version the linked core speaks.
func CoreSchemaVersion() uint32 { return uint32(C.flz_core_schema_version()) }

// WatchStdinEOF ends the process when standard input reaches end of file, on a
// thread of the core's own.
func WatchStdinEOF() { C.flz_core_watch_stdin_eof() }

// HostOwnsSignals keeps the core from installing its SIGTERM and SIGINT
// handler: the Go runtime crashes on a foreign handler without SA_ONSTACK.
func HostOwnsSignals() error {
	return contextFree(func() bool { return C.flz_core_host_owns_signals() != Err })
}

// Terminate ends the runner as the core's own signal handler does, and the
// process with it. It does not return.
func Terminate(reason string) {
	C.flz_core_terminate(C.CString(reason))
}

// TakeNotices hands out the core's notices held since the last take, as JSON.
// The first take starts the holding.
func TakeNotices() []byte {
	var out *C.uint8_t
	var n C.size_t
	C.flz_core_take_notices(&out, &n)
	return take(out, n)
}

// ResponseParts is one Response channel payload as the core's decoder reads it.
type ResponseParts struct {
	Target   string
	Created  bool
	Request  []byte
	Answered bool
	// The answer's bytes, or the JSON error document the call ended in.
	Result []byte
}

// ResponseDecode reads a call envelope.
func ResponseDecode(payload []byte) (ResponseParts, error) {
	out := make([]byte, len(payload)+1)
	parts := make([]C.size_t, 5)
	if err := contextFree(func() bool {
		return C.flz_core_response_decode(bytesPtr(payload), C.size_t(len(payload)),
			(*C.uint8_t)(unsafe.Pointer(&out[0])), &parts[0]) != Err
	}); err != nil {
		return ResponseParts{}, err
	}
	target, request, result := int(parts[0]), int(parts[1]), int(parts[2])
	return ResponseParts{
		Target:   string(out[:target]),
		Request:  append([]byte(nil), out[target:target+request]...),
		Result:   append([]byte(nil), out[target+request:target+request+result]...),
		Created:  parts[3] == 1,
		Answered: parts[4] == 1,
	}, nil
}

// Context is one FlzContext and every call a running host makes against it.
type Context struct {
	ptr *C.FlzContext
}

// NewContext creates a context, refusing a core of another schema version
// before any controller connection.
func NewContext() (*Context, error) { return newContextAt(SchemaVersion) }

func newContextAt(version uint32) (*Context, error) {
	var ptr *C.FlzContext
	if err := contextFree(func() bool {
		ptr = C.flz_core_context_new(C.uint32_t(version))
		return ptr != nil
	}); err != nil {
		return nil, err
	}
	return &Context{ptr: ptr}, nil
}

// Free releases the context. Call it once, after every caller has left.
func (c *Context) Free() {
	if c.ptr != nil {
		C.flz_core_context_free(c.ptr)
		c.ptr = nil
	}
}

// call runs one entry on the context, answering its status and, when it
// failed, the refusal the core recorded for this thread.
func (c *Context) call(entry func() C.int32_t) (int, error) {
	var status C.int32_t
	err := onThread(func() bool {
		status = entry()
		return status != Err
	}, func() *C.char { return C.flz_core_last_error(c.ptr) })
	return int(status), err
}

// callOut is call for an entry that may hand out a buffer: the buffer is taken
// whatever the status, since one the caller does not take nobody frees.
func (c *Context) callOut(entry func(out **C.uint8_t, n *C.size_t) C.int32_t) (int, []byte, error) {
	var out *C.uint8_t
	var n C.size_t
	status, err := c.call(func() C.int32_t { return entry(&out, &n) })
	return status, take(out, n), err
}

// DeclareClass registers one class from its declaration JSON.
func (c *Context) DeclareClass(decl []byte) error {
	_, err := c.call(func() C.int32_t { return C.flz_core_declare_class(c.ptr, bytesPtr(decl), C.size_t(len(decl))) })
	return err
}

// UndeclareClass is the inverse of DeclareClass.
func (c *Context) UndeclareClass(factoryID string) error {
	cID := C.CString(factoryID)
	defer C.free(unsafe.Pointer(cID))
	_, err := c.call(func() C.int32_t { return C.flz_core_undeclare_class(c.ptr, cID) })
	return err
}

// CheckClass judges a declaration without registering it, answering the
// derived declaration as JSON.
func (c *Context) CheckClass(decl []byte) ([]byte, error) {
	_, derived, err := c.callOut(func(out **C.uint8_t, n *C.size_t) C.int32_t {
		return C.flz_core_check_class(c.ptr, bytesPtr(decl), C.size_t(len(decl)), out, n)
	})
	return derived, err
}

// Start connects to the controller and starts the runner loop.
func (c *Context) Start(namespace string) error {
	cNamespace := C.CString(namespace)
	defer C.free(unsafe.Pointer(cNamespace))
	_, err := c.call(func() C.int32_t { return C.flz_core_start(c.ptr, cNamespace) })
	return err
}

// LocalStart starts the declared class as a local run, with no controller.
func (c *Context) LocalStart(factoryID, relayID string) error {
	cFactory, cRelay := C.CString(factoryID), C.CString(relayID)
	defer C.free(unsafe.Pointer(cFactory))
	defer C.free(unsafe.Pointer(cRelay))
	_, err := c.call(func() C.int32_t { return C.flz_core_local_start(c.ptr, cFactory, cRelay) })
	return err
}

// LocalOnSink queues a declared output for LocalNextOutput.
func (c *Context) LocalOnSink(channel string) error {
	cChannel := C.CString(channel)
	defer C.free(unsafe.Pointer(cChannel))
	_, err := c.call(func() C.int32_t { return C.flz_core_local_on_sink(c.ptr, cChannel) })
	return err
}

// LocalFeed feeds one value's wire bytes to a declared entry channel.
func (c *Context) LocalFeed(channel string, payload []byte) error {
	cChannel := C.CString(channel)
	defer C.free(unsafe.Pointer(cChannel))
	_, err := c.call(func() C.int32_t {
		return C.flz_core_local_feed(c.ptr, cChannel, bytesPtr(payload), C.size_t(len(payload)))
	})
	return err
}

// LocalNextOutput is the next value an output released, in poll's three-way shape.
func (c *Context) LocalNextOutput(timeoutMs uint64) (int, []byte, error) {
	return c.callOut(func(out **C.uint8_t, n *C.size_t) C.int32_t {
		return C.flz_core_local_next_output(c.ptr, C.uint64_t(timeoutMs), out, n)
	})
}

// LocalRunUntilIdle parks until the graph is idle; a timeout is a refusal.
func (c *Context) LocalRunUntilIdle(timeoutMs uint64) error {
	_, err := c.call(func() C.int32_t { return C.flz_core_local_run_until_idle(c.ptr, C.uint64_t(timeoutMs)) })
	return err
}

// LocalStop ends the local run.
func (c *Context) LocalStop() error {
	_, err := c.call(func() C.int32_t { return C.flz_core_local_stop(c.ptr) })
	return err
}

// Poll blocks up to timeoutMs for one activation: its status, and the encoded
// activation on PollActivation.
func (c *Context) Poll(timeoutMs uint64) (int, []byte, error) {
	return c.callOut(func(out **C.uint8_t, n *C.size_t) C.int32_t {
		return C.flz_core_poll(c.ptr, C.uint64_t(timeoutMs), out, n)
	})
}

// PoolPolicy is what flz_core_pool_open answers.
type PoolPolicy struct {
	Base, Ceiling                         int
	PollTimeoutMs, RetireMs, DrainBoundMs uint32
}

// PoolOpen opens the context's pool governor over defaultBase.
func (c *Context) PoolOpen(defaultBase int) (PoolPolicy, error) {
	policy := make([]C.uint32_t, 5)
	if _, err := c.call(func() C.int32_t { return C.flz_core_pool_open(c.ptr, C.uint32_t(defaultBase), &policy[0]) }); err != nil {
		return PoolPolicy{}, err
	}
	return PoolPolicy{
		Base: int(policy[0]), Ceiling: int(policy[1]),
		PollTimeoutMs: uint32(policy[2]), RetireMs: uint32(policy[3]), DrainBoundMs: uint32(policy[4]),
	}, nil
}

// PoolSupervise is the governor's next decision, at most a second away.
func (c *Context) PoolSupervise() (int, error) {
	return c.call(func() C.int32_t { return C.flz_core_pool_supervise(c.ptr) })
}

// PoolLeft gives back one worker's seat.
func (c *Context) PoolLeft() error {
	_, err := c.call(func() C.int32_t { return C.flz_core_pool_left(c.ptr) })
	return err
}

// Complete answers an activation with its encoded result.
func (c *Context) Complete(result []byte) error {
	_, err := c.call(func() C.int32_t { return C.flz_core_complete(c.ptr, bytesPtr(result), C.size_t(len(result))) })
	return err
}

// Abandon re-hands a polled activation's untouched items.
func (c *Context) Abandon(token uint64) error {
	_, err := c.call(func() C.int32_t { return C.flz_core_abandon(c.ptr, C.uint64_t(token)) })
	return err
}

// Shutdown ships every cell's state, then wakes every blocked poller.
func (c *Context) Shutdown() { C.flz_core_shutdown(c.ptr) }

// SpawnRequest sends one spawn request of the activation token and answers its
// handle at once. offset < 0 is none.
func (c *Context) SpawnRequest(token uint64, factoryID, id string, payload []byte, ifRunning string, replay bool, offset int64) (uint64, error) {
	cFactory, cID, cIf := C.CString(factoryID), C.CString(id), C.CString(ifRunning)
	defer C.free(unsafe.Pointer(cFactory))
	defer C.free(unsafe.Pointer(cID))
	defer C.free(unsafe.Pointer(cIf))
	var handle C.uint64_t
	_, err := c.call(func() C.int32_t {
		return C.flz_core_spawn_request(c.ptr, C.uint64_t(token), cFactory, cID,
			bytesPtr(payload), C.size_t(len(payload)), cIf, boolInt(replay), C.int64_t(offset), &handle)
	})
	return uint64(handle), err
}

// SpawnResult reads a handle: the answer as JSON, or nil while it is pending.
func (c *Context) SpawnResult(handle, timeoutMs uint64) ([]byte, error) {
	status, answer, err := c.callOut(func(out **C.uint8_t, n *C.size_t) C.int32_t {
		return C.flz_core_spawn_result(c.ptr, C.uint64_t(handle), C.uint64_t(timeoutMs), out, n)
	})
	if err != nil || status == SpawnPending {
		return nil, err
	}
	return answer, nil
}

// Part sends a part of the activation token's work item.
func (c *Context) Part(token uint64, item uint32, payload []byte) error {
	_, err := c.call(func() C.int32_t {
		return C.flz_core_part(c.ptr, C.uint64_t(token), C.uint32_t(item), bytesPtr(payload), C.size_t(len(payload)))
	})
	return err
}

// EffectBegin begins ctx.Effect(name): EffectKept with the kept result, or
// EffectRun / EffectUnrecorded, whose function runs.
func (c *Context) EffectBegin(token uint64, item uint32, name string) (int, []byte, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return c.callOut(func(out **C.uint8_t, n *C.size_t) C.int32_t {
		return C.flz_core_effect_begin(c.ptr, C.uint64_t(token), C.uint32_t(item), cName, out, n)
	})
}

// EffectEnd keeps a result: nil to return the call's own, else the result kept first.
func (c *Context) EffectEnd(token uint64, item uint32, payload []byte) ([]byte, error) {
	_, first, err := c.callOut(func(out **C.uint8_t, n *C.size_t) C.int32_t {
		return C.flz_core_effect_end(c.ptr, C.uint64_t(token), C.uint32_t(item), bytesPtr(payload), C.size_t(len(payload)), out, n)
	})
	return first, err
}

// EffectRelease ends a begun effect without keeping anything.
func (c *Context) EffectRelease(token uint64, item uint32) error {
	_, err := c.call(func() C.int32_t { return C.flz_core_effect_release(c.ptr, C.uint64_t(token), C.uint32_t(item)) })
	return err
}

// AdminCall administers the namespace this connection proved; args and the
// answer are JSON.
func (c *Context) AdminCall(verb string, args []byte) ([]byte, error) {
	cVerb, cArgs := C.CString(verb), C.CString(string(args))
	defer C.free(unsafe.Pointer(cVerb))
	defer C.free(unsafe.Pointer(cArgs))
	_, result, err := c.callOut(func(out **C.uint8_t, n *C.size_t) C.int32_t {
		return C.flz_core_admin_call(c.ptr, cVerb, cArgs, out, n)
	})
	return result, err
}

// SendMessage sends one message, through the controller or locally; the answer
// is JSON. An empty msgID or key is none.
func (c *Context) SendMessage(relayID, channel string, message []byte, msgID, key string, local bool) ([]byte, error) {
	cRelay, cChannel, cMsgID, cKey := C.CString(relayID), C.CString(channel), C.CString(msgID), C.CString(key)
	defer C.free(unsafe.Pointer(cRelay))
	defer C.free(unsafe.Pointer(cChannel))
	defer C.free(unsafe.Pointer(cMsgID))
	defer C.free(unsafe.Pointer(cKey))
	_, result, err := c.callOut(func(out **C.uint8_t, n *C.size_t) C.int32_t {
		return C.flz_core_send_message(c.ptr, cRelay, cChannel, bytesPtr(message), C.size_t(len(message)), cMsgID, cKey, boolInt(local), out, n)
	})
	return result, err
}

// Relays is the JSON list of the relay ids bound on this runner.
func (c *Context) Relays() ([]byte, error) {
	_, ids, err := c.callOut(func(out **C.uint8_t, n *C.size_t) C.int32_t { return C.flz_core_relays(c.ptr, out, n) })
	return ids, err
}

// take copies a buffer the core handed out into Go memory and frees it.
func take(buf *C.uint8_t, n C.size_t) []byte {
	if buf == nil {
		return nil
	}
	copied := C.GoBytes(unsafe.Pointer(buf), C.int(n))
	C.flz_core_buf_free(buf, n)
	return copied
}

// noBytes backs an empty slice: the ABI reads every buffer through a
// non-null pointer, a zero-length one included.
var noBytes [1]byte

func bytesPtr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return (*C.uint8_t)(unsafe.Pointer(&noBytes[0]))
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

func boolInt(b bool) C.int32_t {
	if b {
		return 1
	}
	return 0
}
