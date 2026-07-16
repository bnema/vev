package clipboard

import (
	"encoding/hex"
	"errors"
	"strings"
)

const (
	osaDataPrefix = "«data "
	osaDataSuffix = "»"
	osaClassLen   = 4
)

// parseOSAData decodes osascript's textual representation of an AppleScript
// data value: «data CLASSHEX». osascript terminates successful output with a
// newline, so surrounding whitespace is ignored; all other bytes must belong
// to the data value.
func parseOSAData(output, class string) ([]byte, error) {
	if len(class) != osaClassLen {
		return nil, errors.New("clipboard: invalid expected osascript data class")
	}

	output = strings.TrimSpace(output)
	if !strings.HasPrefix(output, osaDataPrefix) || !strings.HasSuffix(output, osaDataSuffix) {
		return nil, errors.New("clipboard: invalid osascript data output")
	}

	payload := strings.TrimSuffix(strings.TrimPrefix(output, osaDataPrefix), osaDataSuffix)
	if !strings.HasPrefix(payload, class) {
		return nil, errors.New("clipboard: unexpected osascript data class")
	}
	hexData := strings.TrimPrefix(payload, class)
	if hexData == "" {
		return nil, errors.New("clipboard: osascript data output has no data")
	}

	data, err := hex.DecodeString(hexData)
	if err != nil || len(data) == 0 {
		return nil, errors.New("clipboard: invalid osascript data hex")
	}
	return data, nil
}
