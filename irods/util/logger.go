package util

import (
	log "github.com/sirupsen/logrus"
)

var (
	// LogLevel is a level of log
	logLevel int = 9
)

// SetLogLevel sets log level
func SetLogLevel(level int) {
	logLevel = level

	if logLevel >= 8 {
		log.SetLevel(log.DebugLevel)
	} else if logLevel >= 5 {
		log.SetLevel(log.InfoLevel)
	} else if logLevel >= 3 {
		log.SetLevel(log.WarnLevel)
	} else if logLevel >= 1 {
		log.SetLevel(log.ErrorLevel)
	} else {
		log.SetLevel(log.FatalLevel)
	}
}

// GetLogLevel returns current log level
func GetLogLevel() int {
	return logLevel
}

// IsLogLevelError checks if current log level is error
func IsLogLevelError() bool {
	return log.GetLevel() == log.ErrorLevel
}

// IsLogLevelWarn checks if current log level is warn
func IsLogLevelWarn() bool {
	return log.GetLevel() == log.WarnLevel
}

// IsLogLevelInfo checks if current log level is info
func IsLogLevelInfo() bool {
	return log.GetLevel() >= log.InfoLevel
}

// IsLogLevelDebug checks if current log level is debug
func IsLogLevelDebug() bool {
	return log.GetLevel() >= log.DebugLevel
}

// LogDebug logs debug level message
func LogDebug(message interface{}) {
	log.Debug(message)
}

// LogDebugf logs debug level message
func LogDebugf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

// LogInfo logs info level message
func LogInfo(message interface{}) {
	log.Info(message)
}

// LogInfof logs info level message
func LogInfof(format string, v ...interface{}) {
	log.Infof(format, v...)
}

// LogWarn logs warn level message
func LogWarn(message interface{}) {
	log.Warn(message)
}

// LogWarnf logs warn level message
func LogWarnf(format string, v ...interface{}) {
	log.Warnf(format, v...)
}

// LogError logs error level message
func LogError(message interface{}) {
	log.Error(message)
}

// LogErrorf logs error level message
func LogErrorf(format string, v ...interface{}) {
	log.Errorf(format, v...)
}
