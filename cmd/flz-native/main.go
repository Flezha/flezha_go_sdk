// Command flz-native downloads the prebuilt core library the SDK links through
// cgo, checks it against the release's checksums, and prints the CGO_LDFLAGS
// line that points Go at it:
//
//	eval "$(go run github.com/flezha/flezha_go_sdk/cmd/flz-native@v0.1.1)"
//
// It imports nothing from the SDK, whose packages cannot build until the
// library is in place.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
)

const releases = "https://github.com/flezha/flezha/releases/download"

// The platforms a release publishes the library for, by the name its asset
// carries.
var platformDir = map[string]string{
	"linux/amd64":  "linux-x64",
	"linux/arm64":  "linux-arm64",
	"darwin/amd64": "macos-x64",
	"darwin/arm64": "macos-arm64",
}

func main() {
	version := flag.String("version", moduleVersion(), "the release to fetch, such as v0.1.0")
	dir := flag.String("dir", "", "where to put the library (default: a folder in the user cache)")
	flag.Parse()

	out, err := fetch(http.DefaultClient, releases, *version, runtime.GOOS+"/"+runtime.GOARCH, *dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "flz-native:", err)
		os.Exit(1)
	}
	fmt.Printf("export CGO_LDFLAGS=\"-L%s\"\n", out)
}

// A release tag; a checkout build reports a pseudo-version, which names none.
var releaseTag = regexp.MustCompile(`^v\d+\.\d+\.\d+(-rc\.\d+)?$`)

// The version `go run <module>/cmd/flz-native@<version>` was built from.
func moduleVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		return releaseVersion(info.Main.Version)
	}
	return ""
}

func releaseVersion(v string) string {
	if releaseTag.MatchString(v) {
		return v
	}
	return ""
}

// fetch puts the library for platform into dir (or a cache folder named by
// version and platform) and answers that folder. A library already there
// with the release's checksum is kept.
func fetch(client *http.Client, base, version, platform, dir string) (string, error) {
	if version == "" {
		return "", errors.New("no release version: pass -version, or run it as .../cmd/flz-native@<version>")
	}
	name, ok := platformDir[platform]
	if !ok {
		return "", fmt.Errorf("no prebuilt core library for %s; build it from a Flezha checkout with scripts/build-go-sdk.sh", platform)
	}
	if dir == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return "", err
		}
		dir = filepath.Join(cache, "flezha", version, name)
	}

	asset := "libflz_core_ffi-" + name + ".a"
	sums, err := get(client, base+"/"+version+"/SHA256SUMS.txt")
	if err != nil {
		return "", err
	}
	want, err := sumFor(sums, asset)
	if err != nil {
		return "", err
	}

	lib := filepath.Join(dir, "libflz_core_ffi.a")
	if have, err := fileSum(lib); err == nil && have == want {
		return dir, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, download(client, base+"/"+version+"/"+asset, lib, want)
}

func get(client *http.Client, url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// The checksum SHA256SUMS.txt names for asset.
func sumFor(sums []byte, asset string) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(sums))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && strings.TrimPrefix(fields[1], "*") == asset {
			return fields[0], nil
		}
	}
	return "", fmt.Errorf("the release lists no checksum for %s", asset)
}

func fileSum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// download writes url to dst through a temporary file beside it, so a failed
// or mismatched download leaves nothing where the linker looks.
func download(client *http.Client, url, dst, want string) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dst), ".libflz_core_ffi-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(tmp, h), resp.Body)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if have := hex.EncodeToString(h.Sum(nil)); have != want {
		return fmt.Errorf("%s: checksum %s, the release lists %s", url, have, want)
	}
	return os.Rename(tmp.Name(), dst)
}
