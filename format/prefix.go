package format

import (
	"fmt"
	"strings"
)

type PrefixFormatter struct {
}

func (f *PrefixFormatter) Format(fld, op, val string) string {

	fmt.Println("fld: ", fld, "op: ", op, "val: ", val)
	return strings.Trim(val, "\"")
}
func (f *PrefixFormatter) FormatArray(op string, val ...string) string {

	return ""
}
