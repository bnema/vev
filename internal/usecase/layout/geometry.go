package layout

import (
	"math"
	"sort"

	"github.com/bnema/vev/internal/domain"
)

// Solve returns deterministic pane placements for root inside area.
func Solve(root *Node, area domain.Rect) ([]Placement, bool) {
	if root == nil || area.Width <= 0 || area.Height <= 0 {
		return nil, false
	}
	minWidth, ok := minimumExtent(root, Horizontal)
	if !ok || area.Width < minWidth {
		return nil, false
	}
	minHeight, ok := minimumExtent(root, Vertical)
	if !ok || area.Height < minHeight {
		return nil, false
	}
	var out []Placement
	if !solve(root, area, &out) {
		return nil, false
	}
	return out, true
}

func solve(n *Node, r domain.Rect, out *[]Placement) bool {
	switch n.Kind {
	case Leaf:
		if r.Width < MinPaneCols || r.Height < MinPaneRows {
			return false
		}
		*out = append(*out, Placement{ID: n.Leaf, Content: r})
		return true
	case Split:
		return solveSplit(n, r, out)
	case Stack:
		return solveStack(n, r, out)
	default:
		return false
	}
}

func solveSplit(n *Node, r domain.Rect, out *[]Placement) bool {
	rects, ok := splitChildRects(n, r)
	if !ok {
		return false
	}
	for i, child := range n.Children {
		if !solve(child, rects[i], out) {
			return false
		}
	}
	return true
}

func splitChildRects(n *Node, r domain.Rect) ([]domain.Rect, bool) {
	count := len(n.Children)
	if count == 0 {
		return nil, false
	}
	if count == 1 {
		return []domain.Rect{r}, true
	}

	total := r.Width
	if n.Dir == Vertical {
		total = r.Height
	}
	usable := total - (count - 1)
	minimums := make([]int, count)
	weights := make([]float64, count)
	for i, child := range n.Children {
		minimum, ok := minimumExtent(child, n.Dir)
		if !ok {
			return nil, false
		}
		minimums[i] = minimum
		weights[i] = effectiveWeight(child.Weight)
	}
	extents, ok := distribute(usable, minimums, weights)
	if !ok {
		return nil, false
	}

	rects := make([]domain.Rect, count)
	x, y := r.X, r.Y
	for i, extent := range extents {
		if n.Dir == Horizontal {
			rects[i] = domain.Rect{X: x, Y: r.Y, Width: extent, Height: r.Height}
			x += extent + 1
			continue
		}
		rects[i] = domain.Rect{X: r.X, Y: y, Width: r.Width, Height: extent}
		y += extent + 1
	}
	return rects, true
}

func effectiveWeight(weight float64) float64 {
	if weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
		return 1
	}
	return weight
}

func minimumExtent(n *Node, axis SplitDir) (int, bool) {
	if n == nil {
		return 0, false
	}
	switch n.Kind {
	case Leaf:
		if axis == Horizontal {
			return MinPaneCols, true
		}
		return MinPaneRows, true
	case Stack:
		if len(n.Children) == 0 {
			return 0, false
		}
		for _, child := range n.Children {
			if child == nil || child.Kind != Leaf {
				return 0, false
			}
		}
		if axis == Horizontal {
			return MinPaneCols, true
		}
		return len(n.Children) + 1, true
	case Split:
		if len(n.Children) == 0 {
			return 0, false
		}
		if n.Dir == axis {
			total := len(n.Children) - 1
			for _, child := range n.Children {
				minimum, ok := minimumExtent(child, axis)
				if !ok {
					return 0, false
				}
				total += minimum
			}
			return total, true
		}
		largest := 0
		for _, child := range n.Children {
			minimum, ok := minimumExtent(child, axis)
			if !ok {
				return 0, false
			}
			largest = max(largest, minimum)
		}
		return largest, true
	default:
		return 0, false
	}
}

// distribute allocates total integer cells proportionally while respecting each
// child's minimum. It uses stable largest-remainder rounding after all
// constrained children have been pinned.
func distribute(total int, minimums []int, weights []float64) ([]int, bool) {
	if total < 0 || len(minimums) == 0 || len(minimums) != len(weights) {
		return nil, false
	}
	minimumTotal := 0
	for _, minimum := range minimums {
		if minimum < 0 {
			return nil, false
		}
		minimumTotal += minimum
	}
	if minimumTotal > total {
		return nil, false
	}

	allocations := make([]int, len(minimums))
	active := make([]bool, len(minimums))
	for i := range active {
		active[i] = true
	}
	remaining := total

	for {
		largest, sum := activeWeightScale(weights, active)
		var pinned []int
		for i := range active {
			if !active[i] {
				continue
			}
			quota := float64(remaining) * (effectiveWeight(weights[i]) / largest) / sum
			if quota < float64(minimums[i]) {
				pinned = append(pinned, i)
			}
		}
		if len(pinned) == 0 {
			break
		}
		for _, i := range pinned {
			allocations[i] = minimums[i]
			remaining -= minimums[i]
			active[i] = false
		}
	}

	largest, sum := activeWeightScale(weights, active)
	type remainder struct {
		index    int
		fraction float64
	}
	remainders := make([]remainder, 0, len(weights))
	allocated := total - remaining
	for i := range active {
		if !active[i] {
			continue
		}
		quota := float64(remaining) * (effectiveWeight(weights[i]) / largest) / sum
		whole := int(math.Floor(quota))
		allocations[i] = whole
		allocated += whole
		remainders = append(remainders, remainder{index: i, fraction: quota - float64(whole)})
	}
	sort.SliceStable(remainders, func(i, j int) bool {
		return remainders[i].fraction > remainders[j].fraction
	})
	for cells, i := total-allocated, 0; cells > 0; cells, i = cells-1, i+1 {
		allocations[remainders[i%len(remainders)].index]++
	}
	return allocations, true
}

func activeWeightScale(weights []float64, active []bool) (float64, float64) {
	largest := 0.0
	for i, weight := range weights {
		if active[i] {
			largest = math.Max(largest, effectiveWeight(weight))
		}
	}
	sum := 0.0
	for i, weight := range weights {
		if active[i] {
			sum += effectiveWeight(weight) / largest
		}
	}
	return largest, sum
}

func solveStack(n *Node, r domain.Rect, out *[]Placement) bool {
	count := len(n.Children)
	if count == 0 || r.Width < MinPaneCols || r.Height < count+1 {
		return false
	}
	idx := -1
	for i, child := range n.Children {
		if child.Kind != Leaf {
			return false
		}
		if child.Leaf == n.Expanded {
			idx = i
		}
	}
	if idx < 0 {
		return false
	}
	contentRows := r.Height - (count - 1)
	for i, child := range n.Children {
		if i < idx {
			titleY := r.Y + i
			*out = append(*out, Placement{ID: child.Leaf, TitleBar: domain.Rect{X: r.X, Y: titleY, Width: r.Width, Height: 1}, Collapsed: true, InStack: true})
			continue
		}
		if i == idx {
			*out = append(*out, Placement{
				ID:      child.Leaf,
				Content: domain.Rect{X: r.X, Y: r.Y + idx, Width: r.Width, Height: contentRows},
				InStack: true,
			})
			continue
		}
		titleY := r.Y + idx + contentRows + (i - idx - 1)
		*out = append(*out, Placement{ID: child.Leaf, TitleBar: domain.Rect{X: r.X, Y: titleY, Width: r.Width, Height: 1}, Collapsed: true, InStack: true})
	}
	return true
}
