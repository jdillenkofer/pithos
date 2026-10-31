package tlsmanager

import (
	"context"
	"log/slog"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type slogCore struct {
	logger *slog.Logger
	fields []zapcore.Field
}

func newZapLogger(logger *slog.Logger) *zap.Logger {
	return zap.New(&slogCore{logger: logger})
}

func (c *slogCore) Enabled(level zapcore.Level) bool {
	return c.logger.Enabled(context.Background(), slogLevel(level))
}

func (c *slogCore) With(fields []zapcore.Field) zapcore.Core {
	clone := &slogCore{logger: c.logger, fields: make([]zapcore.Field, 0, len(c.fields)+len(fields))}
	clone.fields = append(clone.fields, c.fields...)
	clone.fields = append(clone.fields, fields...)
	return clone
}

func (c *slogCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c *slogCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	encoder := zapcore.NewMapObjectEncoder()
	for _, field := range c.fields {
		field.AddTo(encoder)
	}
	for _, field := range fields {
		field.AddTo(encoder)
	}
	attrs := make([]any, 0, len(encoder.Fields)+2)
	attrs = append(attrs, "component", "acme")
	for key, value := range encoder.Fields {
		attrs = append(attrs, key, value)
	}
	c.logger.Log(context.Background(), slogLevel(entry.Level), entry.Message, attrs...)
	return nil
}

func (c *slogCore) Sync() error { return nil }

func slogLevel(level zapcore.Level) slog.Level {
	switch {
	case level <= zapcore.DebugLevel:
		return slog.LevelDebug
	case level == zapcore.InfoLevel:
		return slog.LevelInfo
	case level == zapcore.WarnLevel:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}
