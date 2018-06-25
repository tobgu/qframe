package maps

import (
	"reflect"
	"sort"

	"github.com/tobgu/qframe/internal/strings"
)

// StringKeys returns a sorted list of all unique keys present in mm.
// This function will panic if mm contains non-maps or maps containing
// other key types than string.
func StringKeys(mm ...interface{}) []string {
	keySet := strings.NewStringSet(nil)
	for _, m := range mm {
		v := reflect.ValueOf(m)
		keys := v.MapKeys()
		for _, k := range keys {
			keySet.Add(k.String())
		}
	}

	result := keySet.AsSlice()
	sort.Strings(result)
	return result
}
