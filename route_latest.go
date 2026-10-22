package flezha

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Routed is one delivery of RouteLatest: the latest document, for one key.
// It travels as the JSON pair [key, document].
type Routed[D any] struct {
	Key      string
	Document D
}

func (r Routed[D]) MarshalJSON() ([]byte, error) { return encodeJSON([]any{r.Key, r.Document}) }

func (r *Routed[D]) UnmarshalJSON(b []byte) error {
	var pair []json.RawMessage
	if err := json.Unmarshal(b, &pair); err != nil {
		return err
	}
	if len(pair) != 2 {
		return fmt.Errorf("a routed delivery is a [key, document] pair, not %d values", len(pair))
	}
	if err := json.Unmarshal(pair[0], &r.Key); err != nil {
		return err
	}
	return json.Unmarshal(pair[1], &r.Document)
}

// RouteLatest turns a relay-wide document stream into per-key deliveries:
// every key seen on traffic receives each distinct document once, on that
// key's own next traffic item. Delivering on the key's own traffic keeps every
// key one this member owns after a rescale; the price is one tick of latency,
// and a key with no traffic receives nothing.
func RouteLatest[D, T any](c *ChannelBuilder, documents Stream[D], traffic Stream[T], key func(T) string) Stream[Routed[D]] {
	return RouteLatestNamed(c, documents, traffic, key, "")
}

// RouteLatestNamed is RouteLatest under an explicit node identity, for a
// second router in one relay.
func RouteLatestNamed[D, T any](c *ChannelBuilder, documents Stream[D], traffic Stream[T], key func(T) string, nodeName string) Stream[Routed[D]] {
	m := NewMerge[routerInput[D]](c)
	From(m, documents, func(d D) routerInput[D] { return routerInput[D]{Document: &d} })
	From(m, Map(traffic, key), func(k string) routerInput[D] { return routerInput[D]{Traffic: &k} })
	return MergeIntoNamed(m, latestRouter[D]{}, nodeName)
}

// routerInput is a document or a traffic item's key; the traffic value itself
// never reaches the router.
type routerInput[D any] struct {
	Document *D      `json:"document,omitempty"`
	Traffic  *string `json:"traffic,omitempty"`
}

// routerState retains the document as the JSON it was encoded to, never
// parsed, and the version each key last received.
type routerState struct {
	Document  json.RawMessage `json:"document"`
	Version   int             `json:"version"`
	Delivered map[string]int  `json:"delivered"`
}

type routerRetain struct {
	Retain json.RawMessage `json:"retain"`
}

func (c routerRetain) ApplyTo(s *routerState) {
	s.Document = c.Retain
	s.Version++
}

type routerDelivered struct {
	Key     string `json:"key"`
	Version int    `json:"version"`
}

func (c routerDelivered) ApplyTo(s *routerState) {
	delivered := make(map[string]int, len(s.Delivered)+1)
	for k, v := range s.Delivered {
		delivered[k] = v
	}
	delivered[c.Key] = c.Version
	s.Delivered = delivered
}

type latestRouter[D any] struct{}

func (latestRouter[D]) Spec() Spec { return Spec{Name: "route_latest"} }

func (latestRouter[D]) InitState(*string) routerState {
	return routerState{Delivered: map[string]int{}}
}

func (latestRouter[D]) Changes() Changes[routerState] {
	return Changes[routerState]{"retain": routerRetain{}, "delivered": routerDelivered{}}
}

func (latestRouter[D]) Run(in routerInput[D], ctx *Ctx[routerState, Routed[D]]) error {
	state := ctx.State()
	// Version 0 means nothing retained, so the first document lands at 1.
	retained := state.Version > 0
	if in.Document != nil {
		encoded, err := encodeJSON(*in.Document)
		if err != nil {
			return err
		}
		if !retained || !bytes.Equal(state.Document, encoded) {
			return ctx.Change(routerRetain{Retain: encoded})
		}
		return nil
	}
	if in.Traffic == nil {
		return nil
	}
	key := *in.Traffic
	if !retained || state.Delivered[key] >= state.Version {
		return nil
	}
	var document D
	if err := json.Unmarshal(state.Document, &document); err != nil {
		return err
	}
	// Emit first: a failed emit rides the retry ladder, and recording the
	// delivery before it would make the retry silent.
	if err := ctx.Emit(Routed[D]{Key: key, Document: document}); err != nil {
		return err
	}
	return ctx.Change(routerDelivered{Key: key, Version: state.Version})
}
