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

func (c *CronLogger) Info(msg string, keysAndValues ...interface{}) {
	c.l.Info(msg, keysAndValues...)
}

func (c *CronLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	c.l.Error(msg, keysAndValues...)
}
