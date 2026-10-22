package codec

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/flezha/flezha_go_sdk/internal/contract"
)

// rows is every `activation` and `result` row of the shared postcard fixture.
func rows(t *testing.T) map[string][]byte {
	t.Helper()
	text, err := contract.Read("test/fixtures/postcard_bodies.txt")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	for _, line := range strings.Split(string(text), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "|")
		if fields[0] != "activation" && fields[0] != "result" {
			continue
		}
		raw, err := hex.DecodeString(fields[2])
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		out[fields[0]+"|"+fields[1]] = raw
	}
	return out
}

func TestEverySchemaFixtureRoundTrips(t *testing.T) {
	cases := rows(t)
	if len(cases) < 11 {
		t.Fatalf("expected the eight activation and three result rows, found %d", len(cases))
	}
	for name, raw := range cases {
		var again []byte
		if strings.HasPrefix(name, "activation|") {
			a, err := DecodeActivation(raw)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			again = EncodeActivation(a)
		} else {
			r, err := DecodeResult(raw)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			again = EncodeResult(r)
		}
		if !bytes.Equal(again, raw) {
			t.Errorf("%s: re-encoded %x, fixture %x", name, again, raw)
		}
	}
}

func TestTheWorkActivationDecodesItsFields(t *testing.T) {
	a, err := DecodeActivation(rows(t)["activation|work"])
	if err != nil {
		t.Fatal(err)
	}
	if a.Token != 42 || a.Kind != KindWork || len(a.Items) != 2 || a.Factory != "f" || string(a.CellIdentity) != "id" {
		t.Fatalf("unexpected decode: %+v", a)
	}
	input := a.Items[0]
	if input.Form != Prepared || len(input.Values) != 1 || input.ConsumedOffset == nil || *input.ConsumedOffset != 7 {
		t.Fatalf("unexpected input: %+v", input)
	}
	if timer := a.Items[1]; !timer.Timer || timer.TimerKind != EventTime || timer.ScheduledAt != 2000 {
		t.Fatalf("unexpected timer: %+v", timer)
	}
}

func TestAU64DecodesIntoAUint64(t *testing.T) {
	w := &writer{}
	w.varint(^uint64(0))
	r := &reader{buf: w.buf}
	if got := r.varint("max"); got != ^uint64(0) {
		t.Fatalf("got %d", got)
	}
}

func TestTrailingBytesAreRefused(t *testing.T) {
	raw := append(rows(t)["activation|leave"], 0)
	if _, err := DecodeActivation(raw); err == nil || !strings.Contains(err.Error(), "unread byte") {
		t.Fatalf("expected a refusal, got %v", err)
	}
}
