package flezha

import (
	"bytes"
	"encoding/json"
	"reflect"
	"unicode/utf8"
)

// encodeJSON is a value as JSON, with no HTML escaping and no trailing newline.
func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// decodeJSONInto decodes b into a T; an empty payload is T's zero value.
func decodeJSONInto[T any](b []byte) (any, error) {
	var v T
	if len(b) == 0 {
		return v, nil
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// sent is a value encoded as JSON, refused naming its owner when it has none.
func sent(owner string, v any) ([]byte, error) {
	out, err := encodeJSON(v)
	if err != nil {
		return nil, refusal("runtime.value_not_json", map[string]any{
			"owner": owner, "type": typeName(reflect.TypeOf(v)), "reason": err.Error(),
		})
	}
	return out, nil
}

// plain is what an output naming no codec sends, and what a completion, a
// spawn payload and an effect result carry: a string or a []byte as it
// stands, anything else as JSON.
func plain(v any, owner string) ([]byte, error) {
	switch v := v.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return sent(owner, v)
	}
}

// decoderFor is an entry channel's decoder for a T under codec, refused at
// declaration when T cannot hold what the codec reads.
func decoderFor[T any](codec ChannelCodec, channel string) (func([]byte) (any, error), error) {
	t := typeOf[T]()
	wrong := func() error {
		return refusal("runtime.value_wrong_for_codec", map[string]any{
			"channel": channel, "codec": string(codec), "type": typeName(t),
		})
	}
	switch codec {
	case JSON:
		return decodeJSONInto[T], nil
	case Text:
		if t.Kind() != reflect.String && t.Kind() != reflect.Interface {
			return nil, wrong()
		}
		return func(b []byte) (any, error) {
			if !utf8.Valid(b) {
				return nil, refusal("runtime.value_wrong_for_codec", map[string]any{
					"channel": channel, "codec": string(Text), "type": "payload that is not valid UTF-8",
				})
			}
			return convert(string(b), t), nil
		}, nil
	case Bytes:
		switch {
		case t.Kind() == reflect.String:
			return func(b []byte) (any, error) { return convert(string(b), t), nil }, nil
		case t == reflect.TypeOf([]byte(nil)) || t.Kind() == reflect.Interface:
			return func(b []byte) (any, error) { return append([]byte(nil), b...), nil }, nil
		}
		return nil, wrong()
	default:
		return nil, refusal("topology.unknown_codec", map[string]any{
			"channel": channel, "codec": string(codec), "codecs": []string{"json", "text", "bytes"},
		})
	}
}

// convert is s as t, for a string type of the author's own.
func convert(s string, t reflect.Type) any {
	if t.Kind() == reflect.String && t != reflect.TypeOf("") {
		return reflect.ValueOf(s).Convert(t).Interface()
	}
	return s
}

// encoderFor is an output's encoder under codec.
func encoderFor(codec ChannelCodec, ch *channelSpec) func(any) ([]byte, error) {
	wrong := func(v any) error {
		what := typeName(reflect.TypeOf(v))
		if _, isString := v.(string); isString {
			what = "string that is not valid UTF-8"
		}
		return refusal("runtime.value_wrong_for_codec", map[string]any{
			"channel": ch.name, "codec": string(codec), "type": what,
		})
	}
	switch codec {
	case Bytes:
		return func(v any) ([]byte, error) {
			switch v := v.(type) {
			case []byte:
				return v, nil
			case string:
				return []byte(v), nil
			}
			return nil, wrong(v)
		}
	case Text:
		return func(v any) ([]byte, error) {
			if s, ok := v.(string); ok && utf8.ValidString(s) {
				return []byte(s), nil
			}
			return nil, wrong(v)
		}
	default:
		return func(v any) ([]byte, error) { return sent("channel '"+ch.name+"'", v) }
	}
}
