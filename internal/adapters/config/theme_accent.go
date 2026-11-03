package config

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bnema/vev/internal/domain"
)

func parseThemeAccent(value string) (domain.ThemeAccent, string) {
	if value == "auto" {
		return domain.ThemeAccent{Mode: domain.ThemeAccentAuto}, ""
	}

	if strings.HasPrefix(value, "rgb:") && len(value) == 10 {
		color, err := strconv.ParseUint(value[4:], 16, 24)
		if err == nil {
			return domain.ThemeAccent{Mode: domain.ThemeAccentRGB, RGB: [3]uint8{uint8(color >> 16), uint8(color >> 8), uint8(color)}}, ""
		}
	}

	slot, err := strconv.Atoi(value)
	if err != nil || slot < 0 || slot > 15 || strconv.Itoa(slot) != value {
		return domain.ThemeAccent{Mode: domain.ThemeAccentAuto}, fmt.Sprintf("invalid theme.accent %q", value)
	}

	accent := domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: uint8(slot)}
	switch slot {
	case 0, 7, 8, 15:
		return accent, fmt.Sprintf("theme.accent slot %d is conventionally neutral", slot)
	default:
		return accent, ""
	}
}
