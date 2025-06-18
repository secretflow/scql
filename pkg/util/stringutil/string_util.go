// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package stringutil

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/exp/rand"

	"github.com/pingcap/errors"

	"github.com/secretflow/scql/pkg/util/hack"
)

// ErrSyntax indicates that a value does not have the right syntax for the target type.
var ErrSyntax = errors.New("invalid syntax")

// UnquoteChar decodes the first character or byte in the escaped string
// or character literal represented by the string s.
// It returns four values:
//
// 1) value, the decoded Unicode code point or byte value;
// 2) multibyte, a boolean indicating whether the decoded character requires a multibyte UTF-8 representation;
// 3) tail, the remainder of the string after the character; and
// 4) an error that will be nil if the character is syntactically valid.
//
// The second argument, quote, specifies the type of literal being parsed
// and therefore which escaped quote character is permitted.
// If set to a single quote, it permits the sequence \' and disallows unescaped '.
// If set to a double quote, it permits \" and disallows unescaped ".
// If set to zero, it does not permit either escape and allows both quote characters to appear unescaped.
// Different with strconv.UnquoteChar, it permits unnecessary backslash.
func UnquoteChar(s string, quote byte) (value []byte, tail string, err error) {
	// easy cases
	switch c := s[0]; {
	case c == quote:
		err = errors.Trace(ErrSyntax)
		return
	case c >= utf8.RuneSelf:
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError {
			value = append(value, c)
			return value, s[1:], nil
		}
		value = append(value, string(r)...)
		return value, s[size:], nil
	case c != '\\':
		value = append(value, c)
		return value, s[1:], nil
	}
	// hard case: c is backslash
	if len(s) <= 1 {
		err = errors.Trace(ErrSyntax)
		return
	}
	c := s[1]
	s = s[2:]
	switch c {
	case 'b':
		value = append(value, '\b')
	case 'n':
		value = append(value, '\n')
	case 'r':
		value = append(value, '\r')
	case 't':
		value = append(value, '\t')
	case 'Z':
		value = append(value, '\032')
	case '0':
		value = append(value, '\000')
	case '_', '%':
		value = append(value, '\\')
		value = append(value, c)
	case '\\':
		value = append(value, '\\')
	case '\'', '"':
		value = append(value, c)
	default:
		value = append(value, c)
	}
	tail = s
	return
}

// Unquote interprets s as a single-quoted, double-quoted,
// or backquoted Go string literal, returning the string value
// that s quotes. For example: test=`"\"\n"` (hex: 22 5c 22 5c 6e 22)
// should be converted to `"\n` (hex: 22 0a).
func Unquote(s string) (t string, err error) {
	n := len(s)
	if n < 2 {
		return "", errors.Trace(ErrSyntax)
	}
	quote := s[0]
	if quote != s[n-1] {
		return "", errors.Trace(ErrSyntax)
	}
	s = s[1 : n-1]
	if quote != '"' && quote != '\'' {
		return "", errors.Trace(ErrSyntax)
	}
	// Avoid allocation. No need to convert if there is no '\'
	if strings.IndexByte(s, '\\') == -1 && strings.IndexByte(s, quote) == -1 {
		return s, nil
	}
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		mb, ss, err := UnquoteChar(s, quote)
		if err != nil {
			return "", errors.Trace(err)
		}
		s = ss
		buf = append(buf, mb...)
	}
	return string(buf), nil
}

const (
	patMatch = iota + 1
	patOne
	patAny
)

// CompilePattern handles escapes and wild cards convert pattern characters and
// pattern types.
func CompilePattern(pattern string, escape byte) (patChars, patTypes []byte) {
	var lastAny bool
	patChars = make([]byte, len(pattern))
	patTypes = make([]byte, len(pattern))
	patLen := 0
	for i := 0; i < len(pattern); i++ {
		var tp byte
		var c = pattern[i]
		switch c {
		case escape:
			lastAny = false
			tp = patMatch
			if i < len(pattern)-1 {
				i++
				c = pattern[i]
				if c == escape || c == '_' || c == '%' {
					// Valid escape.
				} else {
					// Invalid escape, fall back to escape byte.
					// mysql will treat escape character as the origin value even
					// the escape sequence is invalid in Go or C.
					// e.g., \m is invalid in Go, but in MySQL we will get "m" for select '\m'.
					// Following case is correct just for escape \, not for others like +.
					// TODO: Add more checks for other escapes.
					i--
					c = escape
				}
			}
		case '_':
			if lastAny {
				continue
			}
			tp = patOne
		case '%':
			if lastAny {
				continue
			}
			lastAny = true
			tp = patAny
		default:
			lastAny = false
			tp = patMatch
		}
		patChars[patLen] = c
		patTypes[patLen] = tp
		patLen++
	}
	patChars = patChars[:patLen]
	patTypes = patTypes[:patLen]
	return
}

// NOTE: Currently tikv's like function is case sensitive, so we keep its behavior here.
func matchByteCI(a, b byte) bool {
	return a == b
	// We may reuse below code block when like function go back to case insensitive.
	/*
		if a == b {
			return true
		}
		if a >= 'a' && a <= 'z' && a-caseDiff == b {
			return true
		}
		return a >= 'A' && a <= 'Z' && a+caseDiff == b
	*/
}

// CompileLike2Regexp convert a like `lhs` to a regular expression
func CompileLike2Regexp(str string) string {
	patChars, patTypes := CompilePattern(str, '\\')
	var result []byte
	for i := 0; i < len(patChars); i++ {
		switch patTypes[i] {
		case patMatch:
			result = append(result, patChars[i])
		case patOne:
			// .*. == .*
			if !bytes.HasSuffix(result, []byte{'.', '*'}) {
				result = append(result, '.')
			}
		case patAny:
			// ..* == .*
			if bytes.HasSuffix(result, []byte{'.'}) {
				result = append(result, '*')
				continue
			}
			// .*.* == .*
			if !bytes.HasSuffix(result, []byte{'.', '*'}) {
				result = append(result, '.')
				result = append(result, '*')
			}
		}
	}
	return string(result)
}

// DoMatch matches the string with patChars and patTypes.
// The algorithm has linear time complexity.
// https://research.swtch.com/glob
func DoMatch(str string, patChars, patTypes []byte) bool {
	var sIdx, pIdx, nextSIdx, nextPIdx int
	for pIdx < len(patChars) || sIdx < len(str) {
		if pIdx < len(patChars) {
			switch patTypes[pIdx] {
			case patMatch:
				if sIdx < len(str) && matchByteCI(str[sIdx], patChars[pIdx]) {
					pIdx++
					sIdx++
					continue
				}
			case patOne:
				if sIdx < len(str) {
					pIdx++
					sIdx++
					continue
				}
			case patAny:
				// Try to match at sIdx.
				// If that doesn't work out,
				// restart at sIdx+1 next.
				nextPIdx = pIdx
				nextSIdx = sIdx + 1
				pIdx++
				continue
			}
		}
		// Mismatch. Maybe restart.
		if 0 < nextSIdx && nextSIdx <= len(str) {
			pIdx = nextPIdx
			sIdx = nextSIdx
			continue
		}
		return false
	}
	// Matched all of pattern to all of name. Success.
	return true
}

// IsExactMatch return true if no wildcard character
func IsExactMatch(patTypes []byte) bool {
	for _, pt := range patTypes {
		if pt != patMatch {
			return false
		}
	}
	return true
}

// Copy deep copies a string.
func Copy(src string) string {
	return string(hack.Slice(src))
}

// StringerFunc defines string func implement fmt.Stringer.
type StringerFunc func() string

// String implements fmt.Stringer
func (l StringerFunc) String() string {
	return l()
}

// MemoizeStr returns memoized version of stringFunc.
func MemoizeStr(l func() string) fmt.Stringer {
	return StringerFunc(func() string {
		return l()
	})
}

// StringerStr defines a alias to normal string.
// implement fmt.Stringer
type StringerStr string

// String implements fmt.Stringer
func (i StringerStr) String() string {
	return string(i)
}

func RemoveSensitiveInfo(sqlStr string) string {
	exp := regexp.MustCompile(`(?i)\s+IDENTIFIED\s+`)
	if match := exp.FindStringIndex(sqlStr); match != nil {
		return sqlStr[:match[1]] + "***"
	}
	return sqlStr
}

func RandString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	rand.Seed(uint64(time.Now().UnixNano()))
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func IsDateString(s string) bool {
	matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2}(\s\d{2}:\d{2}:\d{2})?$`, s)
	return matched
}

func StringToUnixSec(s string) (int64, error) {
	var t time.Time
	var err error

	if len(s) == len("2006-01-02 15:04:05") {
		t, err = time.Parse("2006-01-02 15:04:05", s)
		if err == nil {
			return t.Unix(), nil
		}
	}

	if len(s) == len("2006-01-02") {
		t, err = time.Parse("2006-01-02", s)
		if err == nil {
			return t.Unix(), nil
		}
	}

	return 0, fmt.Errorf("unsupported datetime format: %s", s)
}

func StringToUnixSecWithTimezone(s string) (int64, error) {
	supportedLayouts := []string{
		// e.g., "2024-10-25T10:00:00+08:00" or "2024-10-25T02:00:00Z"
		time.RFC3339,
		// e.g., "2024-10-25T10:00:00+0800"
		"2006-01-02T15:04:05Z0700",
		// e.g., "2024-10-25 10:00:00+08:00"
		"2006-01-02 15:04:05Z07:00",
		// e.g., "2024-10-25 10:00:00+0800"
		"2006-01-02 15:04:05Z0700",
	}

	var t time.Time
	var err error

	for _, layout := range supportedLayouts {
		t, err = time.Parse(layout, s)
		if err == nil {
			return t.Unix(), nil
		}
	}

	return 0, fmt.Errorf("unsupported timestamp format or missing timezone: %q", s)
}

func MySQLDateFormatToGoLayout(mysqlFormat string) (string, error) {
	var mapping = map[string]string{
		"%Y": "2006",        // Year, four digits
		"%y": "06",          // Year, two digits
		"%m": "01",          // Month (01–12)
		"%c": "1",           // Month (1–12)
		"%d": "02",          // Day (01–31)
		"%e": "2",           // Day (1–31)
		"%H": "15",          // Hour (00–23)
		"%k": "15",          // Hour (0–23)
		"%h": "03",          // Hour (01–12)
		"%I": "03",          // Hour (01–12)
		"%l": "3",           // Hour (1–12)
		"%i": "04",          // Minute (00–59)
		"%S": "05",          // Second (00–59)
		"%s": "05",          // Same as %S
		"%f": "000000",      // Microsecond
		"%p": "PM",          // AM or PM
		"%T": "15:04:05",    // Time in 24-hour format
		"%r": "03:04:05 PM", // Time in 12-hour format
		"%b": "Jan",         // Abbreviated month name
		"%M": "January",     // Full month name
		"%a": "Mon",         // Abbreviated weekday
		"%W": "Monday",      // Full weekday
	}

	var goLayout strings.Builder
	runes := []rune(mysqlFormat)
	i := 0
	for i < len(runes) {
		if runes[i] == '%' {
			if i+1 < len(runes) {
				specifierRune := runes[i+1]
				// Handle %% separately
				if specifierRune == '%' {
					goLayout.WriteRune('%')
					i += 2
					continue
				}
				specifier := "%" + string(specifierRune)
				if layoutPart, ok := mapping[specifier]; ok {
					goLayout.WriteString(layoutPart)
				} else {
					return "", fmt.Errorf("unsupported MySQL format specifier: %s", specifier)
				}
				i += 2
			} else {
				return "", fmt.Errorf("invalid format string: trailing %%")
			}
		} else {
			goLayout.WriteRune(runes[i])
			i += 1
		}
	}
	return goLayout.String(), nil
}

var kMySQLToArrowFormatMapping = map[string]string{
	"%Y": "%Y",          // 4-digit year
	"%y": "%y",          // 2-digit year
	"%m": "%m",          // Month (01-12)
	"%c": "%-m",         // Month (1-12), '%-m' in Arrow is for non-padded month
	"%d": "%d",          // Day (01-31)
	"%e": "%-d",         // Day (1-31), '%-d' in Arrow is for non-padded day
	"%H": "%H",          // Hour (00-23)
	"%k": "%-H",         // Hour (0-23), '%-H' in Arrow is for non-padded hour
	"%h": "%I",          // Hour (01-12), maps to 12-hour format
	"%I": "%I",          // Hour (01-12)
	"%l": "%-I",         // Hour (1-12), '%-I' in Arrow is for non-padded 12-hour
	"%i": "%M",          // MySQL minute -> standard strptime minute (%M)
	"%S": "%S",          // Second (00-59)
	"%s": "%S",          // MySQL second alias -> standard strptime second (%S)
	"%f": "%f",          // Microsecond
	"%p": "%p",          // AM/PM
	"%T": "%H:%M:%S",    // Shortcut: 24-hour time
	"%r": "%I:%M:%S %p", // Shortcut: 12-hour time
	"%b": "%b",          // Abbreviated month name
	"%M": "%B",          // MySQL full month name -> standard strptime full month name (%B)
	"%a": "%a",          // Abbreviated weekday name
	"%W": "%A",          // MySQL full weekday name -> standard strptime full weekday name (%A)
}

func MySQLDateFormatToArrowFormat(mysqlFormat string) (string, error) {
	var result strings.Builder
	i := 0
	for i < len(mysqlFormat) {
		if mysqlFormat[i] == '%' {
			// Check for a dangling '%' at the end of the string.
			if i+1 >= len(mysqlFormat) {
				return "", fmt.Errorf("invalid format string: trailing %%")
			}

			// Special case '%%', which represents a literal '%' character.
			if mysqlFormat[i+1] == '%' {
				result.WriteByte('%')
				i += 2
				continue
			}

			// Extract the two-character specifier, e.g., "%Y".
			specifier := mysqlFormat[i : i+2]
			arrowSpecifier, ok := kMySQLToArrowFormatMapping[specifier]
			if !ok {
				return "", fmt.Errorf("unsupported MySQL format specifier: %s", specifier)
			}

			result.WriteString(arrowSpecifier)
			i += 2
		} else {
			// Non-'%' characters are treated as literals and appended directly.
			result.WriteByte(mysqlFormat[i])
			i++
		}
	}
	return result.String(), nil
}
