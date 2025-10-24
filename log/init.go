package log

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger

const (
	defaultLogLevel = InfoLevel
	defaultLogPath  = "/data/logs"
	FileName        = "app.log"
	DebugLevel      = "debug"
	InfoLevel       = "info"
	WarnLevel       = "warn"
)

func Init(level, path string) {
	// log level
	if level == "" {
		level = defaultLogLevel
	}
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	default:
		panic(fmt.Sprintf("unknown log level: %s", level))
	}
	// log file
	if path == "" {
		path = defaultLogPath
	}
	logFile := GetFullLogPath(path, FileName)
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	fileWriter, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintf("open log file failed: %s", err))
	}
	multi := zerolog.MultiLevelWriter(consoleWriter, fileWriter)
	logger = zerolog.New(multi).With().Timestamp().Logger()
}

func GetFullLogPath(path, fileName string) string {
	if HasSuffix(path, "/") {
		return path + fileName
	}
	return path + "/" + fileName
}

func HasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
