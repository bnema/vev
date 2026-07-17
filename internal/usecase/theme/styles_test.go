package theme

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestNewStylesComposesSemanticStyles(t *testing.T) {
	theme := Theme{
		Foreground: renderer.RGB{R: 0xc8, G: 0xc8, B: 0xc8},
		Background: renderer.RGB{R: 0x0a, G: 0x14, B: 0x1e},
		HasFG:      true,
		HasBG:      true,
		Known:      true,
		TrueColor:  true,
	}

	styles := NewStyles(theme)
	statusBar := StatusBarStyle(theme)
	accent := AccentStyle(theme)
	selection := SelectionStyle(theme)

	require.True(t, styles.StatusBar.Equal(statusBar))
	require.True(t, styles.Accent.Equal(accent))
	require.True(t, styles.Border.Equal(BorderStyle(theme)))
	require.True(t, styles.Selection.Equal(selection))
	require.True(t, styles.CopyStatus.Equal(selection))
	require.True(t, styles.PaletteDesc.Equal(MutedTextStyle(theme)))
	require.True(t, styles.TabName.Equal(EmphasisStyle(statusBar, theme)))
	require.True(t, styles.TabNameActive.Equal(EmphasisStyle(accent, theme)))
	require.True(t, styles.TabTitle.Equal(MutedVariantStyle(statusBar, theme)))
	require.True(t, styles.TabTitleActive.Equal(MutedVariantStyle(accent, theme)))
	require.True(t, styles.PickerName.Equal(EmphasisStyle(renderer.DefaultStyle(), theme)))
	require.True(t, styles.PickerSelectionName.Equal(EmphasisStyle(selection, theme)))
	require.True(t, styles.PickerSelectionMuted.Equal(MutedVariantStyle(selection, theme)))
	require.True(t, styles.PickerSeparator.Equal(MutedTextStyle(theme)))
}
