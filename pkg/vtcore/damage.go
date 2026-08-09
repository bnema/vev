package vtcore

type DamageKind int

const (
	DamageText DamageKind = iota
	DamageClear
	DamageScrollUp
	DamageFullRedraw
)

type Damage struct {
	Kind   DamageKind
	X      int
	Y      int
	Width  int
	Height int
	Count  int
}

func FullRedraw() Damage { return Damage{Kind: DamageFullRedraw, Count: 1} }
