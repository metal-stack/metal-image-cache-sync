package utils

import "go.uber.org/zap"

type CronLogger struct {
	l *zap.SugaredLogger
}

func NewCronLogger(logger *zap.SugaredLogger) *CronLogger {
	return &CronLogger{
		l: logger,
	}
}

func (c *CronLogger) Info(msg string, keysAndValues ...interface{}) {
	c.l.Infow(msg, keysAndValues)
}

func (c *CronLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	c.l.Errorw(msg, keysAndValues)
}
