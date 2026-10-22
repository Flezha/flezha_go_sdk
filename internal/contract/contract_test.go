package contract

import (
	"strings"
	"testing"
)

func TestTheCoreTreeHoldsTheContractSet(t *testing.T) {
	t.Setenv(Env, "")
	if _, err := Dir(); err != nil {
		t.Fatal(err)
	}
}

func TestTheEnvironmentDirectoryComesFirst(t *testing.T) {
	core, err := Dir()
	if err != nil {
		t.Fatal(err)
	}
	other := t.TempDir()
	if dir, _ := search([]string{core, other}); dir != core {
		t.Fatalf("got %s, want %s", dir, core)
	}
}

func TestAMissingSetFailsNamingThePlacesSearched(t *testing.T) {
	places := []string{t.TempDir(), t.TempDir(), ""}
	_, err := search(places)
	if err == nil {
		t.Fatal("a search of three empty places found a contract set")
	}
	for _, want := range []string{places[0], places[1], "$" + Env + " (unset)"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not name %s: %v", want, err)
		}
	}
}
