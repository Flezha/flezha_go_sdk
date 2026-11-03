package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A release server holding one asset, whose SHA256SUMS.txt lists sum for it.
func release(t *testing.T, asset string, body []byte, sum string) (*httptest.Server, *int) {
	t.Helper()
	downloads := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v0.1.0/SHA256SUMS.txt":
			w.Write([]byte("0000  other.tar.gz\n" + sum + "  " + asset + "\n"))
		case "/v0.1.0/" + asset:
			downloads++
			w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &downloads
}

func sumOf(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestTheLibraryIsFetchedVerifiedAndKept(t *testing.T) {
	lib := []byte("!<arch>\nstand-in archive")
	srv, downloads := release(t, "libflz_core_ffi-linux-x64.a", lib, sumOf(lib))
	dir := filepath.Join(t.TempDir(), "lib")

	for range 2 {
		got, err := fetch(srv.Client(), srv.URL, "v0.1.0", "linux/amd64", dir)
		if err != nil || got != dir {
			t.Fatalf("fetch = %q, %v; want %q", got, err, dir)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "libflz_core_ffi.a")); string(b) != string(lib) {
		t.Fatalf("library = %q", b)
	}
	if *downloads != 1 {
		t.Fatalf("downloaded %d times; a library with the release's checksum is kept", *downloads)
	}
}

func TestAMismatchedDownloadLeavesNothingBehind(t *testing.T) {
	srv, _ := release(t, "libflz_core_ffi-macos-arm64.a", []byte("tampered"), sumOf([]byte("genuine")))
	dir := t.TempDir()

	_, err := fetch(srv.Client(), srv.URL, "v0.1.0", "darwin/arm64", dir)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("err = %v; want a checksum mismatch", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("left behind: %v", entries)
	}
}

func TestAnUnpublishedPlatformPointsAtTheCheckoutBuild(t *testing.T) {
	_, err := fetch(http.DefaultClient, "http://unused", "v0.1.0", "windows/amd64", t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "scripts/build-go-sdk.sh") {
		t.Fatalf("err = %v", err)
	}
}

func TestNoVersionSaysHowToName(t *testing.T) {
	_, err := fetch(http.DefaultClient, "http://unused", "", "linux/amd64", t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "-version") {
		t.Fatalf("err = %v", err)
	}
}

func TestAnAssetTheReleaseDoesNotListIsRefused(t *testing.T) {
	srv, _ := release(t, "libflz_core_ffi-linux-x64.a", nil, "abc")
	_, err := fetch(srv.Client(), srv.URL, "v0.1.0", "linux/arm64", t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "no checksum") {
		t.Fatalf("err = %v", err)
	}
}

func TestOnlyAReleaseTagNamesARelease(t *testing.T) {
	for v, want := range map[string]string{
		"v0.1.0":      "v0.1.0",
		"v0.2.0-rc.1": "v0.2.0-rc.1",
		"v0.0.0-20261029084046-d7469f7369a3+dirty": "",
		"(devel)": "",
		"":        "",
	} {
		if got := releaseVersion(v); got != want {
			t.Errorf("releaseVersion(%q) = %q; want %q", v, got, want)
		}
	}
}
