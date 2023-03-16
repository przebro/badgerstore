package format

import (
	"strings"
)

type PrefixFormatter struct {
}

func (f *PrefixFormatter) Format(fld, op, val string) string {
	return strings.Trim(val, "\"")
}
func (f *PrefixFormatter) FormatArray(op string, val ...string) string {

	return ""
}
