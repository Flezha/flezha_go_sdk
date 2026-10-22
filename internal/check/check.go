// Package check is the seam the conformance run judges a relay through: the
// SDK fills Derived at init, and the test app reads the declaration the core
// derived for a relay, with nothing registered and no runner.
package check

// Derived builds relay's declaration and answers what the core derived from
// it, as JSON, or the core's refusal.
var Derived func(relay any) ([]byte, error)
