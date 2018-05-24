package strings

import (
	"strings"

	"github.com/tobgu/qframe/errors"
)

func isQuoted(s string) bool {
	return len(s) > 2 &&
		((strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
			(strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)))
}

func CheckName(name string) error {
	if len(name) == 0 {
		return errors.New("CheckName", "column name must not be empty")
	}

	if isQuoted(name) {
		// Reserved for future use
		return errors.New("CheckName", "column name must not be quoted: %s", name)
	}

	// Reserved for future use of variables in Eval
	if strings.HasPrefix(name, "$") {
		return errors.New("CheckName", "column name must not start with $: %s", name)
	}

	return nil
}
