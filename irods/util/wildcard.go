package util

import (
	"regexp"
	"strings"

	"github.com/dlclark/regexp2"
)

func HasWildcards(input string) bool {
	return (regexp.MustCompile(`(?:[^\\])(?:\\\\)*[?*]`).MatchString(input) ||
		regexp.MustCompile(`^(?:\\\\)*[?*]`).MatchString(input) ||
		regexp.MustCompile(`(?:[^\\])(?:\\\\)*\[.*?(?:[^\\])(?:\\\\)*\]`).MatchString(input) ||
		regexp.MustCompile(`^(?:\\\\)*\[.*?(?:[^\\])(?:\\\\)*\]`).MatchString(input))
}

func UnixWildcardsToSQLWildcards(input string) string {
	output := input
	length := len(input)
	// Use regexp2 rather than regexp here in order to be able to use lookbehind assertions
	//
	// Escape SQL wildcard characters
	output = strings.ReplaceAll(output, "%", `\%`)
	output = strings.ReplaceAll(output, "_", `\_`)
	// Replace ranges with a wildcard
	output, _ = regexp2.MustCompile(`(?<!\\)(?:\\\\)*\[.*?(?<!\\)(?:\\\\)*\]`, regexp2.RE2).Replace(output, `_`, 0, length)
	// Replace non-escaped regular wildcard characters with SQL equivalents
	output, _ = regexp2.MustCompile(`(?<!\\)(?:\\\\)*(\*)`, regexp2.RE2).Replace(output, `%`, 0, length)
	output, _ = regexp2.MustCompile(`(?<!\\)(?:\\\\)*(\?)`, regexp2.RE2).Replace(output, `_`, 0, length)
	return output
}
