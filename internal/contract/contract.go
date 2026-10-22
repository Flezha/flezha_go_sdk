// Package contract finds the contract set: the fixtures under `test/fixtures`
// and the error catalogue every SDK is held to, which live in the core's tree.
package contract

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Env names a directory holding the contract set, ahead of the other places.
const Env = "FLEZHA_CONTRACT_DIR"

// marker is a member every contract set holds.
const marker = "test/fixtures/abi_surface.txt"

// Dir is the root of the contract set: $FLEZHA_CONTRACT_DIR, else `contract/`
// in this module, else the core tree the module sits in. It fails naming the
// places searched rather than letting a fixture test skip.
func Dir() (string, error) {
	module := moduleRoot()
	return search([]string{
		os.Getenv(Env),
		filepath.Join(module, "contract"),
		filepath.Join(module, "..", ".."),
	})
}

// Read is a member's bytes, such as `test/fixtures/postcard_bodies.txt`.
func Read(member string) ([]byte, error) {
	dir, err := Dir()
	if err != nil {
		return nil, err
	}
	return os.ReadFile(filepath.Join(dir, member))
}

func search(places []string) (string, error) {
	for _, dir := range places {
		if dir == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, marker)); err == nil {
			return filepath.Clean(dir), nil
		}
	}
	named := make([]string, len(places))
	for i, dir := range places {
		if dir == "" {
			dir = "$" + Env + " (unset)"
		}
		named[i] = filepath.Clean(dir)
	}
	return "", fmt.Errorf("no contract set found: looked for %s under %s", marker, strings.Join(named, ", "))
}

// moduleRoot is the directory holding this module's go.mod.
func moduleRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}
