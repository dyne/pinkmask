package log

import (
	"io"
	"log"
	"os"
)

type Level int

const (
	LevelInfo Level = iota
	LevelDebug
)

type Logger struct {
	level Level
	info  *log.Logger
	debug *log.Logger
}

func New(level Level, out io.Writer) *Logger {
	if out == nil {
		out = os.Stdout
	}
	return &Logger{
		level: level,
		info:  log.New(out, "INFO: ", log.LstdFlags),
		debug: log.New(out, "DEBUG: ", log.LstdFlags),
	}
}

func (l *Logger) Infof(format string, args ...any) {
	l.info.Printf(format, args...)
}

func (l *Logger) Debugf(format string, args ...any) {
	if l.level >= LevelDebug {
		l.debug.Printf(format, args...)
	}
}

func (l *Logger) Level() Level {
	return l.level
}
