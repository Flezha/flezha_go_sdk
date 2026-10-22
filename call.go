package flezha

import (
	"github.com/flezha/flezha_go_sdk/internal/native"
)

// CallSpec is a call to another relay at the end of a chain. Target names an
// instance, or a partition group whose member the key chooses; Factory names
// a factory whose instance the key names instead, started when absent unless
// NoCreate. DeadlineMs is required. Channel is the callee's entry channel,
// AnswerChannel the channel whose output answers (its first output for the
// request when empty), and Codec the format of the request and the answer,
// JSON by default.
type CallSpec struct {
	Target        string
	Factory       string
	DeadlineMs    int64
	NoCreate      bool
	Channel       string
	AnswerChannel string
	Codec         ChannelCodec
}

// Response is one outcome of a call, as its Response channel delivers it: the
// request the call end sent, the answer as Value or the Err the call ended in
// (a missed deadline is call.timeout), the callee's instance id, and whether
// this call started the callee.
type Response[Req, Ans any] struct {
	Request Req
	Value   Ans
	Err     *Error
	Target  string
	Created bool
}

// Ok answers whether the call was answered.
func (r Response[Req, Ans]) Ok() bool { return r.Err == nil }

// Call ends this edge's chain in a call to another relay, and answers the
// stream of its Response channel, the entry channel `<channel>.response`. Every
// value reaching the call, after this chain's transforms and key, is one
// request. A call is not an edge, so the Response stream may feed the calling
// tasklet.
func Call[Req, Ans any](s Stream[Req], spec CallSpec) Stream[Response[Req, Ans]] {
	b := s.b
	codec := spec.Codec
	if codec == "" {
		codec = JSON
	}
	if s.from.node == nil {
		b.fail(refusal("topology.output_on_entry", map[string]any{"channel": s.from.entry.name}))
		return Stream[Response[Req, Ans]]{b: b, from: s.from}
	}
	ch := s.channel()
	response := ch.name + ".response"
	link := map[string]any{"link": "call", "deadline_ms": spec.DeadlineMs, "create": !spec.NoCreate, "response": response}
	for key, value := range map[string]string{"target": spec.Target, "factory": spec.Factory, "channel": spec.Channel, "answer_channel": spec.AnswerChannel} {
		if value != "" {
			link[key] = value
		}
	}
	b.linkChain(s.from, s.pending.chain, link, nil)
	if ch.call == nil {
		ch.call = &callEnd{transforms: s.pending.transforms, keyBy: s.pending.keyBy, encode: encoderFor(codec, ch)}
	}
	decodeRequest, err := decoderFor[Req](codec, response)
	if err != nil {
		b.fail(err)
	}
	decodeAnswer, err := decoderFor[Ans](codec, response)
	if err != nil {
		b.fail(err)
	}
	entry := b.entry(response, "custom", decodeResponse[Req, Ans](decodeRequest, decodeAnswer))
	return Stream[Response[Req, Ans]]{b: b, from: source{entry: entry}}
}

// decodeResponse reads one Response channel payload: the request the call
// sent, and the answer or the error the call ended in.
func decodeResponse[Req, Ans any](decodeRequest, decodeAnswer func([]byte) (any, error)) func([]byte) (any, error) {
	return func(payload []byte) (any, error) {
		parts, err := native.ResponseDecode(payload)
		if err != nil {
			return nil, err
		}
		request, err := decodeRequest(parts.Request)
		if err != nil {
			return nil, err
		}
		out := Response[Req, Ans]{Request: as[Req](request), Target: parts.Target, Created: parts.Created}
		if !parts.Answered {
			out.Err = native.FromDocument(parts.Result)
			return out, nil
		}
		answer, err := decodeAnswer(parts.Result)
		if err != nil {
			return nil, err
		}
		out.Value = as[Ans](answer)
		return out, nil
	}
}
