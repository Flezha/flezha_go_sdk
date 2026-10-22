package flezha

import (
	"reflect"
	"testing"
)

func TestGivenDropsOnlyWhatWasNotGiven(t *testing.T) {
	var hooks []any
	got := given(map[string]any{
		"none": nil, "empty": "", "off": false, "hooks": hooks,
		"id": "x", "on": true, "opts": map[string]any{}, "zero": 0,
	})
	want := map[string]any{"id": "x", "on": true, "opts": map[string]any{}, "zero": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
