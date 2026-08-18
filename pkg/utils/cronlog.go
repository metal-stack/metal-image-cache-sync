package utils

import "log/slog"

type CronLogger struct {
	l *slog.Logger
}

func NewCronLogger(logger *slog.Logger) *CronLogger {
	return &CronLogger{
		l: logger,
	}
}

func (c *CronLogger) Info(msg string, keysAndValues ...any) {
	c.l.Info(msg, keysAndValues...)
}

func (c *CronLogger) Error(err error, msg string, keysAndValues ...any) {
	c.l.Error(msg, keysAndValues...)
}
