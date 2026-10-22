package flezha

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"

	"github.com/flezha/flezha_go_sdk/internal/native"
)

// defaultLogger writes to standard error at FLEZHA_LOG_LEVEL, warn by default:
// a runner that says nothing about a wedge is the failure diagnostics exist to
// close, and one that narrates every activation is noise.
func defaultLogger() *slog.Logger {
	level := levelNamed(os.Getenv("FLEZHA_LOG_LEVEL"), slog.LevelWarn)
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

// levelNamed is the slog level a Flezha level name spells, or otherwise.
func levelNamed(name string, otherwise slog.Level) slog.Level {
	switch strings.ToLower(name) {
	case "debug", "trace":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return otherwise
}

// drainNotices writes the core's notices held since the last take through
// logger, at their own level: an error and the local-mode banner at error,
// which no level hides.
func drainNotices(logger *slog.Logger) {
	taken := native.TakeNotices()
	if len(taken) == 0 {
		return
	}
	var notices []struct {
		Level  string `json:"level"`
		Target string `json:"target"`
		Text   string `json:"text"`
	}
	if err := json.Unmarshal(taken, &notices); err != nil {
		logger.Error("the core's notices did not decode", "error", err)
		return
	}
	for _, n := range notices {
		// An error and the local-mode banner ("always") are written at error.
		logger.Log(context.Background(), levelNamed(n.Level, slog.LevelError), n.Text, "target", n.Target)
	}
}
