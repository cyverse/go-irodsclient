package util

import (
	"log"
	"os"
)

var (
	// DebugLogger ...
	DebugLogger *log.Logger = log.New(os.Stderr, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	// WarningLogger ...
	WarningLogger *log.Logger = log.New(os.Stderr, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	// InfoLogger ...
	InfoLogger *log.Logger = log.New(os.Stderr, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	// ErrorLogger ...
	ErrorLogger *log.Logger = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
)

// LogDebug logs debug level message
func LogDebug(message interface{}) {
	DebugLogger.Print(message)
}

// LogDebugf logs debug level message
func LogDebugf(format string, v ...interface{}) {
	DebugLogger.Printf(format, v...)
}

// LogInfo logs info level message
func LogInfo(message interface{}) {
	InfoLogger.Print(message)
}

// LogInfof logs info level message
func LogInfof(format string, v ...interface{}) {
	InfoLogger.Printf(format, v...)
}

// LogWarning logs warning level message
func LogWarning(message interface{}) {
	WarningLogger.Print(message)
}

// LogWarningf logs warning level message
func LogWarningf(format string, v ...interface{}) {
	WarningLogger.Printf(format, v...)
}

// LogError logs error level message
func LogError(message interface{}) {
	ErrorLogger.Print(message)
}

// LogErrorf logs error level message
func LogErrorf(format string, v ...interface{}) {
	ErrorLogger.Printf(format, v...)
}
