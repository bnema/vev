package daemon

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev-vt/graphics"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
)

// Graphics output is deliberately a small attachment-local backend. It emits
// direct uploads and ordinary placements from immutable pane snapshots. The
// attachment-local state is also the ownership boundary for Kitty IDs: raw
// scene IDs are never sent to the outer terminal.
const (
	maxGraphicsRecordBytes = 8 << 20
	maxGraphicsOutputBytes = 8 << 20
)

var (
	errGraphicsOutputTooLarge = errors.New("kitty graphics output exceeds bound")
	errGraphicsIDExhausted    = errors.New("kitty graphics output ID exhausted")
)

type graphicsOutputAsset struct {
	id uint64
}

type graphicsOutputPlacement struct {
	id     uint64
	asset  string
	source graphics.PixelRect
	dest   graphics.PixelRect
	cells  graphics.CellRect
	layer  int64
	// transform is set for composed multi-pane placements. Keeping it on the
	// placement (rather than on graphicsOutputState) lets panes with distinct
	// pixel geometries share one attachment-local output state.
	transform    graphicsOutputTransform
	transformSet bool
}

type graphicsOutputState struct {
	assets        map[string]graphicsOutputAsset
	placements    map[string]graphicsOutputPlacement
	pendingImages map[uint64]struct{}
	pendingPlaces map[uint64]struct{}
	// nextID is a per-attachment cursor in Kitty's terminal-global 32-bit ID
	// namespace. Image and placement IDs share the cursor so neither kind can
	// collide within an attachment. namespaceBase is reserved by the daemon for
	// the attachment/session until cleanup has retired its outer objects.
	nextID         uint64
	namespaceBase  uint64
	namespaceFence uint64
	// mayHaveEmitted is sticky once a Kitty record reaches the transport
	// boundary. Output side effects are not terminal-ACKed, so neither a
	// successful socket send nor an empty committed scene can make this
	// namespace safe for another attachment.
	mayHaveEmitted bool
	transform      graphicsOutputTransform
	transformSet   bool
}

type preparedGraphicsOutput struct {
	owner         *graphicsOutputState
	state         graphicsOutputState
	data          []byte
	valid         bool
	sendAttempted bool
}

type graphicsOutputTransform struct {
	originX, originY int
	sourceGeometry   domain.Geometry
}

const (
	maxKittyGraphicsID = uint64(^uint32(0))
	maxGraphicsIDs     = uint64(1024 + 65536)
	// A namespace is deliberately larger than the maximum bounded scene so an
	// attachment can replace/delete objects without sharing IDs with another
	// attachment. The high bits identify the attachment/session namespace and
	// the low bits are its monotonically allocated object IDs. IDs start at one,
	// so reserve only complete blocks and leave the short tail of Kitty's
	// uint32 ID space unused.
	graphicsIDNamespaceSize  = uint64(1 << 17)
	graphicsIDNamespaceCount = maxKittyGraphicsID / graphicsIDNamespaceSize
)

var (
	standaloneGraphicsNamespace atomic.Uint64
	graphicsNamespaceFence      atomic.Uint64
)

type graphicsNamespaceQuarantine struct {
	base  uint64
	fence uint64
}

func nextGraphicsNamespaceFence() uint64 {
	fence := graphicsNamespaceFence.Add(1)
	if fence == 0 {
		fence = graphicsNamespaceFence.Add(1)
	}
	return fence
}

func newGraphicsOutputState() *graphicsOutputState {
	block := (standaloneGraphicsNamespace.Add(1) - 1) % graphicsIDNamespaceCount
	base := block*graphicsIDNamespaceSize + 1
	return newGraphicsOutputStateWithLease(base, nextGraphicsNamespaceFence())
}

func newGraphicsOutputStateWithBase(base uint64) *graphicsOutputState {
	return newGraphicsOutputStateWithLease(base, nextGraphicsNamespaceFence())
}

func newGraphicsOutputStateWithLease(base, fence uint64) *graphicsOutputState {
	maxBase := (graphicsIDNamespaceCount-1)*graphicsIDNamespaceSize + 1
	if base == 0 || (base-1)%graphicsIDNamespaceSize != 0 || base > maxBase {
		base = 1
	}
	if fence == 0 {
		fence = nextGraphicsNamespaceFence()
	}
	return &graphicsOutputState{
		assets:         make(map[string]graphicsOutputAsset),
		placements:     make(map[string]graphicsOutputPlacement),
		pendingImages:  make(map[uint64]struct{}),
		pendingPlaces:  make(map[uint64]struct{}),
		nextID:         base,
		namespaceBase:  base,
		namespaceFence: fence,
	}
}

func graphicsNamespaceKey(sess *session, clientID [16]byte) string {
	if sess == nil {
		return fmt.Sprintf("attachment:%x", clientID)
	}
	return fmt.Sprintf("session:%s:%x:%x", sess.id, sess.incarnation, clientID)
}

// reserveGraphicsNamespaceLocked chooses a deterministic preferred block and
// linearly probes the bounded namespace table for a free block. Caller holds
// d.mu; probing makes equal session/client identities collision-safe too.
func (d *Daemon) reserveGraphicsNamespaceLocked(key string) uint64 {
	base, _ := d.reserveGraphicsNamespaceLeaseLocked(key)
	return base
}

func (d *Daemon) reserveGraphicsNamespaceLeaseLocked(key string) (uint64, uint64) {
	if d == nil {
		return 0, 0
	}
	if d.graphicsNamespaces == nil {
		d.graphicsNamespaces = make(map[uint64]struct{})
	}
	if d.graphicsNamespaceFences == nil {
		d.graphicsNamespaceFences = make(map[uint64]uint64)
	}
	if d.graphicsNamespaceQuarantines == nil {
		d.graphicsNamespaceQuarantines = make(map[uint64]*graphicsNamespaceQuarantine)
	}
	hashInput := []byte(key)
	if d.graphicsNamespaceSalt != 0 {
		salt := make([]byte, 8, 8+len(key))
		binary.BigEndian.PutUint64(salt, d.graphicsNamespaceSalt)
		hashInput = append(salt, hashInput...)
	}
	digest := sha256.Sum256(hashInput)
	preferred := binary.BigEndian.Uint64(digest[:8]) % graphicsIDNamespaceCount
	for offset := uint64(0); offset < graphicsIDNamespaceCount; offset++ {
		block := (preferred + offset) % graphicsIDNamespaceCount
		if _, exists := d.graphicsNamespaces[block]; exists {
			continue
		}
		fence := nextGraphicsNamespaceFence()
		d.graphicsNamespaces[block] = struct{}{}
		d.graphicsNamespaceFences[block] = fence
		return block*graphicsIDNamespaceSize + 1, fence
	}
	return 0, 0
}

func (d *Daemon) releaseGraphicsNamespace(base uint64) {
	if d == nil || base == 0 || base%graphicsIDNamespaceSize != 1 {
		return
	}
	d.mu.Lock()
	d.releaseGraphicsNamespaceLocked(base)
	d.mu.Unlock()
}

// releaseGraphicsNamespaceLocked is used by resume publication while d.mu is
// already held. A quarantined block is intentionally not released by this
// legacy base-only helper; only its fenced cleanup lifecycle may retire it.
func (d *Daemon) releaseGraphicsNamespaceLocked(base uint64) {
	if d == nil || base == 0 || base%graphicsIDNamespaceSize != 1 {
		return
	}
	block := (base - 1) / graphicsIDNamespaceSize
	if _, quarantined := d.graphicsNamespaceQuarantines[block]; quarantined {
		return
	}
	delete(d.graphicsNamespaces, block)
	delete(d.graphicsNamespaceFences, block)
}

func (d *Daemon) releaseGraphicsNamespaceLeaseLocked(state *graphicsOutputState) {
	if d == nil || state == nil || state.namespaceBase == 0 || state.namespaceBase%graphicsIDNamespaceSize != 1 {
		return
	}
	block := (state.namespaceBase - 1) / graphicsIDNamespaceSize
	if _, quarantined := d.graphicsNamespaceQuarantines[block]; quarantined {
		return
	}
	if current := d.graphicsNamespaceFences[block]; current != 0 && current != state.namespaceFence {
		return
	}
	delete(d.graphicsNamespaces, block)
	delete(d.graphicsNamespaceFences, block)
}

// quarantineGraphicsNamespaceLocked fences one exact namespace instance for
// the daemon lifetime. Output side effects have no terminal ACK, so transport
// completion must never release a block that may have reached an outer terminal.
// The caller holds d.mu.
func (d *Daemon) quarantineGraphicsNamespaceLocked(state *graphicsOutputState) *graphicsNamespaceQuarantine {
	if d == nil || state == nil || state.namespaceBase == 0 || state.namespaceBase%graphicsIDNamespaceSize != 1 {
		return nil
	}
	block := (state.namespaceBase - 1) / graphicsIDNamespaceSize
	if d.graphicsNamespaces == nil {
		d.graphicsNamespaces = make(map[uint64]struct{})
	}
	if d.graphicsNamespaceFences == nil {
		d.graphicsNamespaceFences = make(map[uint64]uint64)
	}
	if d.graphicsNamespaceQuarantines == nil {
		d.graphicsNamespaceQuarantines = make(map[uint64]*graphicsNamespaceQuarantine)
	}
	if _, exists := d.graphicsNamespaceQuarantines[block]; exists {
		return nil
	}
	d.graphicsNamespaces[block] = struct{}{}
	d.graphicsNamespaceFences[block] = state.namespaceFence
	q := &graphicsNamespaceQuarantine{base: state.namespaceBase, fence: state.namespaceFence}
	d.graphicsNamespaceQuarantines[block] = q
	return q
}

func cloneGraphicsOutputState(in *graphicsOutputState) graphicsOutputState {
	out := graphicsOutputState{
		assets:        make(map[string]graphicsOutputAsset),
		placements:    make(map[string]graphicsOutputPlacement),
		pendingImages: make(map[uint64]struct{}),
		pendingPlaces: make(map[uint64]struct{}),
		nextID:        1,
	}
	if in == nil {
		return out
	}
	out.nextID, out.namespaceBase = in.nextID, in.namespaceBase
	out.namespaceFence = in.namespaceFence
	out.mayHaveEmitted = in.mayHaveEmitted
	out.transform, out.transformSet = in.transform, in.transformSet
	for key, asset := range in.assets {
		out.assets[key] = asset
	}
	for key, placement := range in.placements {
		out.placements[key] = placement
	}
	for id := range in.pendingImages {
		out.pendingImages[id] = struct{}{}
	}
	for id := range in.pendingPlaces {
		out.pendingPlaces[id] = struct{}{}
	}
	return out
}

// prepare speculatively computes the records needed to make an outer Kitty
// terminal match snapshot. The maps are copied and become visible only when
// commit is called after the enclosing Output record has been sent.
func (s *graphicsOutputState) prepare(snapshot *graphics.Snapshot, reset bool) (*preparedGraphicsOutput, error) {
	return s.prepareWithTransform(snapshot, reset, graphicsOutputTransform{})
}

func (s *graphicsOutputState) prepareWithTransform(snapshot *graphics.Snapshot, reset bool, transform graphicsOutputTransform, viewports ...graphics.Viewport) (*preparedGraphicsOutput, error) {
	assets, placements := graphicsSnapshotRecords(snapshot, viewports...)
	return s.prepareWithRecords(assets, placements, reset, transform)
}

// prepareWithRecords is the same speculative state transition as the Phase 5
// single-scene path, but accepts a composed set of immutable pane records. It
// intentionally does not write to any graphics.Scene.
func (s *graphicsOutputState) prepareWithRecords(assets map[string]graphics.AssetView, placements map[string]graphicsOutputPlacement, reset bool, transform graphicsOutputTransform) (*preparedGraphicsOutput, error) {
	prepared, err := s.prepareWithRecordsOnce(assets, placements, reset, transform, false)
	if !errors.Is(err, errGraphicsIDExhausted) {
		return prepared, err
	}
	// A bounded attachment can safely recycle its own IDs only after deleting
	// the objects it owns. Keep this retry attachment-local: it never advances
	// into the next reserved namespace block.
	return s.prepareWithRecordsOnce(assets, placements, true, transform, true)
}

func (s *graphicsOutputState) prepareWithTransformOnce(snapshot *graphics.Snapshot, reset bool, transform graphicsOutputTransform, resetIDs bool, viewports ...graphics.Viewport) (*preparedGraphicsOutput, error) {
	assets, placements := graphicsSnapshotRecords(snapshot, viewports...)
	return s.prepareWithRecordsOnce(assets, placements, reset, transform, resetIDs)
}

func (s *graphicsOutputState) prepareWithRecordsOnce(assets map[string]graphics.AssetView, placements map[string]graphicsOutputPlacement, reset bool, transform graphicsOutputTransform, resetIDs bool) (*preparedGraphicsOutput, error) {
	if s == nil {
		return nil, nil
	}
	candidate := cloneGraphicsOutputState(s)
	transformChanged := !s.transformSet || s.transform != transform
	replay := reset || transformChanged
	candidate.transform, candidate.transformSet = transform, true
	out := &preparedGraphicsOutput{owner: s, state: candidate, valid: true}
	if replay {
		for _, id := range sortedUint64Keys(s.pendingPlaces) {
			if err := out.record(deletePlacementRecord(id)); err != nil {
				return nil, err
			}
		}
		for _, id := range sortedUint64Keys(s.pendingImages) {
			if err := out.record(deleteImageRecord(id)); err != nil {
				return nil, err
			}
		}
		for _, placement := range sortedOutputPlacements(s.placements) {
			if err := out.record(deletePlacementRecord(placement.id)); err != nil {
				return nil, err
			}
		}
		for _, asset := range sortedOutputAssets(s.assets) {
			if err := out.record(deleteImageRecord(asset.id)); err != nil {
				return nil, err
			}
		}
		// Keep attachment-owned IDs stable across a rebase. The old remote
		// objects are deleted first, then the same IDs are uploaded and placed
		// again in the replay records below.
		candidate.pendingPlaces = make(map[uint64]struct{})
		candidate.pendingImages = make(map[uint64]struct{})
		if resetIDs {
			candidate.assets = make(map[string]graphicsOutputAsset)
			candidate.placements = make(map[string]graphicsOutputPlacement)
			candidate.nextID = candidate.namespaceBase
		}
	}

	for key, placement := range candidate.placements {
		if _, ok := placements[key]; ok {
			continue
		}
		if err := out.record(deletePlacementRecord(placement.id)); err != nil {
			return nil, err
		}
		delete(candidate.placements, key)
	}
	for key, asset := range candidate.assets {
		if _, ok := assets[key]; ok {
			continue
		}
		if err := out.record(deleteImageRecord(asset.id)); err != nil {
			return nil, err
		}
		delete(candidate.assets, key)
	}

	assetKeys := sortedAssetKeys(assets)
	for _, key := range assetKeys {
		if _, ok := candidate.assets[key]; ok && !replay {
			continue
		}
		asset, exists := candidate.assets[key]
		id := asset.id
		if !exists {
			var ok bool
			id, ok = takeGraphicsIDInNamespace(&candidate.nextID, candidate.namespaceBase)
			if !ok {
				return nil, errGraphicsIDExhausted
			}
			candidate.assets[key] = graphicsOutputAsset{id: id}
		}
		if err := out.record(uploadRecord(id, assets[key])); err != nil {
			return nil, err
		}
	}

	placementKeys := sortedPlacementKeys(placements)
	for _, key := range placementKeys {
		want := placements[key]
		got, exists := candidate.placements[key]
		if exists && equalGraphicsPlacement(got, want) && !replay {
			continue
		}
		if exists {
			if !replay {
				if err := out.record(deletePlacementRecord(got.id)); err != nil {
					return nil, err
				}
			}
			want.id = got.id
		} else {
			id, ok := takeGraphicsIDInNamespace(&candidate.nextID, candidate.namespaceBase)
			if !ok {
				return nil, errGraphicsIDExhausted
			}
			want.id = id
		}
		asset, ok := candidate.assets[want.asset]
		if !ok {
			// A malformed scene cannot make the outer terminal emit a dangling
			// placement. The VT graphics adapter normally prevents this state.
			delete(candidate.placements, key)
			continue
		}
		candidate.placements[key] = want
		placementTransform := transform
		if want.transformSet {
			placementTransform = want.transform
		}
		if err := out.record(placementRecordWithTransform(asset.id, want, placementTransform)); err != nil {
			return nil, err
		}
	}
	// candidate.nextID and any maps replaced during namespace recovery changed
	// after out was initialized; publish the complete speculative value before
	// commit copies it back to the attachment owner.
	out.state = candidate
	if len(out.data) == 0 && !reset {
		out.data = nil
	}
	return out, nil
}

func (p *preparedGraphicsOutput) record(record []byte) error {
	if len(record) > maxGraphicsRecordBytes || len(p.data) > maxGraphicsOutputBytes-len(record) {
		return errGraphicsOutputTooLarge
	}
	p.data = append(p.data, record...)
	return nil
}

func (p *preparedGraphicsOutput) commit() {
	if p == nil || !p.valid || p.owner == nil {
		return
	}
	*p.owner = p.state
	p.valid = false
}

func (p *preparedGraphicsOutput) markSendAttempted() {
	if p != nil && p.valid && p.owner != nil && len(p.data) > 0 {
		p.sendAttempted = true
		p.owner.mayHaveEmitted = true
		p.state.mayHaveEmitted = true
	}
}

func (p *preparedGraphicsOutput) abort() {
	if p == nil || !p.valid || p.owner == nil {
		return
	}
	// A preparation can be abandoned before the enclosing Output frame ever
	// reaches the transport (for example when ANSI preparation fails). Such IDs
	// were never emitted and must not become cleanup records. Once the transport
	// was actually called, its result is ambiguous: the terminal may have
	// consumed a prefix, so retain those speculative IDs for the next reset.
	if p.sendAttempted {
		for key, asset := range p.state.assets {
			if committed, exists := p.owner.assets[key]; !exists || committed.id != asset.id {
				p.owner.pendingImages[asset.id] = struct{}{}
			}
		}
		for key, placement := range p.state.placements {
			if committed, exists := p.owner.placements[key]; !exists || committed.id != placement.id {
				p.owner.pendingPlaces[placement.id] = struct{}{}
			}
		}
	}
	p.valid = false
}

func graphicsSnapshotRecords(snapshot *graphics.Snapshot, viewports ...graphics.Viewport) (map[string]graphics.AssetView, map[string]graphicsOutputPlacement) {
	return graphicsSnapshotRecordsForSource(snapshot, "", viewports...)
}

// graphicsSnapshotRecordsForSource projects one immutable pane scene into
// attachment-local records. source is part of every key because a child VT
// scene is allowed to issue the same raw a1/p1 IDs as every other pane.
// viewports are a deterministic union of visible regions; this is used to
// subtract a floating pane from tiled content without mutating the scene.
func graphicsSnapshotRecordsForSource(snapshot *graphics.Snapshot, source string, viewports ...graphics.Viewport) (map[string]graphics.AssetView, map[string]graphicsOutputPlacement) {
	return graphicsSnapshotRecordsForSourceOrdered(snapshot, source, 0, viewports...)
}

func graphicsSnapshotRecordsForSourceOrdered(snapshot *graphics.Snapshot, source string, paneOrder int, viewports ...graphics.Viewport) (map[string]graphics.AssetView, map[string]graphicsOutputPlacement) {
	assets := make(map[string]graphics.AssetView)
	placements := make(map[string]graphicsOutputPlacement)
	if snapshot == nil {
		return assets, placements
	}
	assetKeys := make(map[string]string)
	generation := snapshot.Generation()
	for _, asset := range snapshot.Assets() {
		key := graphicsAssetKeyForSource(source, generation, asset)
		assets[key] = asset
		assetKeys[asset.ID().String()] = key
	}

	placementViews := snapshot.Placements()
	if len(viewports) == 0 {
		for order, placement := range placementViews {
			addGraphicsOutputPlacementForSourceOrdered(placements, source, paneOrder, order, generation, assetKeys, placement.ID().String(), placement.AssetID().String(), placement.Source(), placement.Destination(), placement.Cells(), placement.Layer())
		}
		return assets, placements
	}

	byPlacementID := make(map[string]graphics.PlacementView, len(placementViews))
	placementOrders := make(map[string]int, len(placementViews))
	for order, placement := range placementViews {
		id := placement.ID().String()
		byPlacementID[id] = placement
		placementOrders[id] = order
	}
	for _, viewport := range viewports {
		cellFragments := make(map[string]graphics.VisibleFragment)
		if viewport.Cells.Valid() && !viewport.Cells.Empty() {
			for _, fragment := range snapshot.VisibleFragments(viewport) {
				cellFragments[fragment.PlacementID.String()] = fragment
			}
		}
		if viewport.Pixels.Valid() && !viewport.Pixels.Empty() {
			for _, fragment := range snapshot.VisiblePixelFragments(viewport.Pixels) {
				placement, ok := byPlacementID[fragment.PlacementID.String()]
				if !ok {
					continue
				}
				if placement.Cells().Valid() && !placement.Cells().Empty() {
					fragment, ok = cellFragments[fragment.PlacementID.String()]
					if !ok {
						continue
					}
				}
				order := placementOrders[fragment.PlacementID.String()]
				addGraphicsOutputPlacementForSourceOrdered(placements, source, paneOrder, order, generation, assetKeys, fragment.PlacementID.String(), fragment.AssetID.String(), fragment.Source, fragment.Destination, fragment.Cells, placement.Layer())
			}
			continue
		}
		// A cell-only viewport is useful to callers that do not know pixel
		// geometry. Cell-aware placements already carry enough information for
		// the graphics package to map the clipped destination back to pixels.
		for _, fragment := range cellFragments {
			placement, ok := byPlacementID[fragment.PlacementID.String()]
			if !ok {
				continue
			}
			order := placementOrders[fragment.PlacementID.String()]
			addGraphicsOutputPlacementForSourceOrdered(placements, source, paneOrder, order, generation, assetKeys, fragment.PlacementID.String(), fragment.AssetID.String(), fragment.Source, fragment.Destination, fragment.Cells, placement.Layer())
		}
	}
	// A viewport is an attachment-local visibility boundary. Do not upload
	// assets that have no visible placement in that boundary: a pane can retain
	// decoded Kitty images after its last placement was erased, but those image
	// records must not leak through a covered or collapsed projection.
	if len(viewports) != 0 {
		usedAssets := make(map[string]struct{}, len(placements))
		for _, placement := range placements {
			usedAssets[placement.asset] = struct{}{}
		}
		for key := range assets {
			if _, ok := usedAssets[key]; !ok {
				delete(assets, key)
			}
		}
	}
	return assets, placements
}

func addGraphicsOutputPlacement(dst map[string]graphicsOutputPlacement, generation uint64, assetKeys map[string]string, placementID, assetID string, source, destination graphics.PixelRect, cells graphics.CellRect, layer int64) {
	addGraphicsOutputPlacementForSource(dst, "", 0, generation, assetKeys, placementID, assetID, source, destination, cells, layer)
}

func addGraphicsOutputPlacementForSource(dst map[string]graphicsOutputPlacement, source string, order int, generation uint64, assetKeys map[string]string, placementID, assetID string, sourceRect, destination graphics.PixelRect, cells graphics.CellRect, layer int64) {
	addGraphicsOutputPlacementForSourceOrdered(dst, source, 0, order, generation, assetKeys, placementID, assetID, sourceRect, destination, cells, layer)
}

func addGraphicsOutputPlacementForSourceOrdered(dst map[string]graphicsOutputPlacement, source string, paneOrder, order int, generation uint64, assetKeys map[string]string, placementID, assetID string, sourceRect, destination graphics.PixelRect, cells graphics.CellRect, layer int64) {
	assetKey := assetKeys[assetID]
	if assetKey == "" {
		return
	}
	key := graphicsPlacementKeyPartsForSourceOrdered(source, paneOrder, order, generation, placementID, assetKey, sourceRect, destination, cells, layer)
	dst[key] = graphicsOutputPlacement{asset: assetKey, source: sourceRect, dest: destination, cells: cells, layer: layer}
}

func graphicsAssetKey(generation uint64, asset graphics.AssetView) string {
	return graphicsAssetKeyForSource("", generation, asset)
}

func graphicsAssetKeyForSource(source string, generation uint64, asset graphics.AssetView) string {
	digest := sha256.Sum256(asset.Encoded())
	if source == "" {
		return fmt.Sprintf("g%d:%s:%d:%d:%x", generation, asset.ID().String(), asset.Width(), asset.Height(), digest)
	}
	return fmt.Sprintf("s:%s:g%d:%s:%d:%d:%x", source, generation, asset.ID().String(), asset.Width(), asset.Height(), digest)
}

func graphicsPlacementKey(generation uint64, placement graphics.PlacementView, assetKey string) string {
	return graphicsPlacementKeyParts(generation, placement.ID().String(), assetKey, placement.Source(), placement.Destination(), placement.Cells(), placement.Layer())
}

func graphicsPlacementKeyParts(generation uint64, placementID, assetKey string, source, dest graphics.PixelRect, cells graphics.CellRect, layer int64) string {
	return graphicsPlacementKeyPartsForSource("", 0, generation, placementID, assetKey, source, dest, cells, layer)
}

func graphicsPlacementKeyPartsForSource(source string, order int, generation uint64, placementID, assetKey string, sourceRect, dest graphics.PixelRect, cells graphics.CellRect, layer int64) string {
	return graphicsPlacementKeyPartsForSourceOrdered(source, 0, order, generation, placementID, assetKey, sourceRect, dest, cells, layer)
}

func graphicsPlacementKeyPartsForSourceOrdered(source string, paneOrder, order int, generation uint64, placementID, assetKey string, sourceRect, dest graphics.PixelRect, cells graphics.CellRect, layer int64) string {
	if source == "" {
		return fmt.Sprintf("g%d:%s:%s:%v:%v:%v:%d", generation, placementID, assetKey, sourceRect, dest, cells, layer)
	}
	// Put the fixed-width pane/scene order first so map iteration is irrelevant:
	// placements are replayed in layout/scene order, while the remaining fields
	// still distinguish clipped fragments and scene generations. The source
	// identity itself deliberately excludes order so moving a pane can retain
	// its attachment-local uploaded asset and only replace its placement.
	return fmt.Sprintf("o%08d:%08d:s:%s:g%d:%s:%s:%v:%v:%v:%d", paneOrder, order, source, generation, placementID, assetKey, sourceRect, dest, cells, layer)
}

func sortedOutputAssets(values map[string]graphicsOutputAsset) []graphicsOutputAsset {
	out := make([]graphicsOutputAsset, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func sortedOutputPlacements(values map[string]graphicsOutputPlacement) []graphicsOutputPlacement {
	out := make([]graphicsOutputPlacement, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func sortedAssetKeys(values map[string]graphics.AssetView) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedPlacementKeys(values map[string]graphicsOutputPlacement) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedUint64Keys(values map[uint64]struct{}) []uint64 {
	keys := make([]uint64, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func takeGraphicsID(next *uint64) (uint64, bool) {
	if next == nil || *next == 0 || *next > maxKittyGraphicsID {
		return 0, false
	}
	base := ((*next - 1) / graphicsIDNamespaceSize) * graphicsIDNamespaceSize
	return takeGraphicsIDInNamespace(next, base+1)
}

func takeGraphicsIDInNamespace(next *uint64, namespaceBase uint64) (uint64, bool) {
	if next == nil || namespaceBase == 0 || (namespaceBase-1)%graphicsIDNamespaceSize != 0 {
		return 0, false
	}
	// IDs are derived from the attachment's reserved block, never from the
	// global Kitty ceiling. Exhaustion marks the cursor unusable; it must never
	// wrap or consume the first ID reserved for another attachment.
	limit := namespaceBase + graphicsIDNamespaceSize
	if limit < namespaceBase || limit > maxKittyGraphicsID+1 || *next < namespaceBase || *next >= limit {
		return 0, false
	}
	id := *next
	if id+1 >= limit {
		*next = 0
	} else {
		(*next)++
	}
	return id, true
}

func equalGraphicsPlacement(a, b graphicsOutputPlacement) bool {
	return a.asset == b.asset && a.source == b.source && a.dest == b.dest && a.cells == b.cells && a.layer == b.layer && a.transformSet == b.transformSet && (!a.transformSet || a.transform == b.transform)
}

func kittyRecord(header string, payload []byte) []byte {
	// q=2 asks Kitty to suppress protocol responses. Outer terminal replies
	// would otherwise be interpreted as pane input or pollute the attachment's
	// byte-only transport.
	if header != "" {
		header += ",q=2"
	}
	out := make([]byte, 0, len(header)+len(payload)+8)
	out = append(out, "\x1b_G"...)
	out = append(out, header...)
	out = append(out, ';')
	out = append(out, payload...)
	out = append(out, "\x1b\\"...)
	return out
}

func deletePlacementRecord(id uint64) []byte {
	return kittyRecord("a=d,d=p,p="+strconv.FormatUint(id, 10), nil)
}

func deleteImageRecord(id uint64) []byte {
	return kittyRecord("a=d,d=i,i="+strconv.FormatUint(id, 10), nil)
}

func uploadRecord(id uint64, asset graphics.AssetView) []byte {
	encoded := asset.Encoded()
	payload := base64.RawStdEncoding.EncodeToString(encoded)
	format := "32"
	if bytes.HasPrefix(encoded, []byte("\x89PNG\r\n\x1a\n")) {
		format = "100"
	} else if asset.Width() > 0 && asset.Height() > 0 &&
		uint64(asset.Width()) <= ^uint64(0)/uint64(asset.Height())/3 &&
		uint64(len(encoded)) == uint64(asset.Width())*uint64(asset.Height())*3 {
		format = "24"
	}
	header := "a=t,i=" + strconv.FormatUint(id, 10) + ",f=" + format +
		",s=" + strconv.FormatInt(asset.Width(), 10) + ",v=" + strconv.FormatInt(asset.Height(), 10)
	return kittyRecord(header, []byte(payload))
}

func placementRecord(assetID uint64, placement graphicsOutputPlacement) []byte {
	return placementRecordWithTransform(assetID, placement, graphicsOutputTransform{})
}

func placementRecordWithTransform(assetID uint64, placement graphicsOutputPlacement, transform graphicsOutputTransform) []byte {
	destinationX, destinationY, cells := transformedGraphicsPlacement(placement, transform)
	header := "a=p,i=" + strconv.FormatUint(assetID, 10) + ",p=" + strconv.FormatUint(placement.id, 10)
	if destinationX >= 0 && destinationY >= 0 {
		header += ",x=" + strconv.FormatInt(destinationX, 10) + ",y=" + strconv.FormatInt(destinationY, 10)
	}
	if placement.source.X >= 0 && placement.source.Y >= 0 {
		header += ",X=" + strconv.FormatInt(placement.source.X, 10) + ",Y=" + strconv.FormatInt(placement.source.Y, 10)
	}
	header += ",w=" + strconv.FormatInt(placement.source.Width, 10) + ",h=" + strconv.FormatInt(placement.source.Height, 10)
	if cells.Valid() && !cells.Empty() {
		header += ",c=" + strconv.FormatInt(cells.Width, 10) + ",r=" + strconv.FormatInt(cells.Height, 10)
		if cells.X != 0 {
			header += ",H=" + strconv.FormatInt(cells.X, 10)
		}
		if cells.Y != 0 {
			header += ",V=" + strconv.FormatInt(cells.Y, 10)
		}
	}
	if placement.layer != 0 {
		header += ",z=" + strconv.FormatInt(placement.layer, 10)
	}
	return kittyRecord(header, nil)
}

// transformedGraphicsPlacement converts pane-local graphics coordinates into
// the outer terminal's one-pane content origin. Kitty's x/y controls are cell
// coordinates; the VT scene stores them in pixels when the pane knows pixel
// geometry and in cells otherwise.
func transformedGraphicsPlacement(placement graphicsOutputPlacement, transform graphicsOutputTransform) (int64, int64, graphics.CellRect) {
	destinationX, destinationY := placement.dest.X, placement.dest.Y
	cells := placement.cells
	if transform.sourceGeometry.PixelsKnown() && transform.sourceGeometry.Cols > 0 && transform.sourceGeometry.Rows > 0 {
		cellWidth := transform.sourceGeometry.PixelWidth / transform.sourceGeometry.Cols
		cellHeight := transform.sourceGeometry.PixelHeight / transform.sourceGeometry.Rows
		if cellWidth > 0 && cellHeight > 0 && destinationX >= 0 && destinationY >= 0 {
			destinationX /= int64(cellWidth)
			destinationY /= int64(cellHeight)
		}
	}
	if destinationX >= 0 {
		destinationX += int64(transform.originX)
	}
	if destinationY >= 0 {
		destinationY += int64(transform.originY)
	}
	return destinationX, destinationY, cells
}

func (s *graphicsOutputState) cleanup() {
	if s == nil {
		return
	}
	s.assets = make(map[string]graphicsOutputAsset)
	s.placements = make(map[string]graphicsOutputPlacement)
	s.pendingImages = make(map[uint64]struct{})
	s.pendingPlaces = make(map[uint64]struct{})
	s.transform = graphicsOutputTransform{}
	s.transformSet = false
}

func graphicsOutputViewport(content domain.Rect, geometry domain.Geometry) (graphics.Viewport, bool) {
	return graphicsOutputViewportRect(domain.Rect{Width: content.Width, Height: content.Height}, geometry)
}

func graphicsOutputViewportRect(content domain.Rect, geometry domain.Geometry) (graphics.Viewport, bool) {
	if content.Width <= 0 || content.Height <= 0 || content.X < 0 || content.Y < 0 {
		return graphics.Viewport{}, false
	}
	cells := graphics.CellRect{X: int64(content.X), Y: int64(content.Y), Width: int64(content.Width), Height: int64(content.Height)}
	if !geometry.PixelsKnown() || geometry.Cols <= 0 || geometry.Rows <= 0 {
		// Cell dimensions are not pixel dimensions. When the controlling
		// terminal did not report pixels, keep clipping in cell space so a
		// pixel-space placement without cell metadata is suppressed by the
		// graphics scene rather than clipped to a made-up pixel rectangle.
		return graphics.Viewport{Cells: cells}, true
	}
	cellWidth := geometry.PixelWidth / geometry.Cols
	cellHeight := geometry.PixelHeight / geometry.Rows
	if cellWidth <= 0 || cellHeight <= 0 {
		return graphics.Viewport{Cells: cells}, true
	}
	return graphics.Viewport{
		Pixels: graphics.PixelRect{
			X:      int64(content.X) * int64(cellWidth),
			Y:      int64(content.Y) * int64(cellHeight),
			Width:  int64(content.Width) * int64(cellWidth),
			Height: int64(content.Height) * int64(cellHeight),
		},
		Cells: cells,
	}, true
}

func graphicsOutputNamespaceMustQuarantine(state *graphicsOutputState) bool {
	return state != nil && (state.mayHaveEmitted || len(state.assets) != 0 || len(state.placements) != 0 || len(state.pendingImages) != 0 || len(state.pendingPlaces) != 0)
}

func (d *Daemon) discardGraphicsOutput(ac *attachedClient) {
	if d == nil || ac == nil || ac.graphicsOutput == nil {
		return
	}
	d.mu.Lock()
	d.retireGraphicsOutputLocked(ac, ac.graphicsOutput)
	d.mu.Unlock()
}

func (d *Daemon) retireGraphicsOutput(ac *attachedClient, state *graphicsOutputState) {
	if d == nil || state == nil {
		return
	}
	d.mu.Lock()
	d.retireGraphicsOutputLocked(ac, state)
	d.mu.Unlock()
}

// retireGraphicsOutputLocked fences exactly state, not whatever graphics state
// a later attachment may have installed on the same client object. Any lease
// that may have crossed the transport boundary stays quarantined for the daemon
// lifetime; only a provably unused reservation is released. The caller holds
// d.mu.
func (d *Daemon) retireGraphicsOutputLocked(ac *attachedClient, state *graphicsOutputState) *graphicsNamespaceQuarantine {
	if d == nil || state == nil {
		return nil
	}
	if ac != nil && ac.graphicsOutput == state {
		ac.graphicsOutput = nil
	}
	quarantine := graphicsOutputNamespaceMustQuarantine(state)
	state.cleanup()
	if !quarantine {
		d.releaseGraphicsNamespaceLeaseLocked(state)
		return nil
	}
	return d.quarantineGraphicsNamespaceLocked(state)
}

// discardGraphicsOutputLocked is used by lifecycle publication paths that
// already hold d.mu. Since those paths cannot establish terminal receipt, any
// namespace that may have emitted remains quarantined.
func (d *Daemon) discardGraphicsOutputLocked(ac *attachedClient) {
	if d == nil || ac == nil || ac.graphicsOutput == nil {
		return
	}
	d.retireGraphicsOutputLocked(ac, ac.graphicsOutput)
}

// cleanupGraphicsOutput removes the objects owned by a closing attachment
// before its transport is closed. Parked resumable attachments intentionally do
// not call this: their state remains attached to the parked link and the next
// epoch deletes and replays it as needed. Even a successful cleanup socket send
// cannot release an exposed namespace because side-effect Output frames are not
// terminal-ACKed.
func (d *Daemon) cleanupGraphicsOutput(ac *attachedClient) error {
	if d == nil || ac == nil {
		return nil
	}
	// Keep preparation, the side-effect frame, and retirement under one
	// attachment send fence. A concurrent paint must not commit a newer scene
	// between the delete preparation and the handoff control frame.
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	state := ac.graphicsOutput
	if state == nil {
		return nil
	}
	if ac.output == nil {
		d.retireGraphicsOutput(ac, state)
		return nil
	}
	prepared, err := state.prepare(nil, true)
	if err != nil || prepared == nil {
		d.retireGraphicsOutput(ac, state)
		return err
	}
	if len(prepared.data) == 0 {
		prepared.commit()
		d.retireGraphicsOutput(ac, state)
		return nil
	}
	expected := ac.transportSnapshot()
	if expected.transport == nil {
		prepared.markSendAttempted()
		prepared.abort()
		d.retireGraphicsOutput(ac, state)
		return errors.New("client transport is nil")
	}
	ac.output.lockView()
	frame, frameErr := ac.output.sideEffectLocked(prepared.data, ac.echoAck.Load())
	if frameErr != nil {
		ac.output.unlockView()
		prepared.abort()
		d.retireGraphicsOutput(ac, state)
		return frameErr
	}
	prepared.markSendAttempted()
	var sendErr error
	if owned, ok := expected.transport.(ports.OwnedSynchronousTransport); ok {
		sendErr = owned.SendSynchronous(frame)
	} else {
		_, sendErr = d.boundedSendWith(expected.transport, func() error {
			if !ac.transportSnapshotCurrent(expected) {
				return errTransportReplaced
			}
			return expected.transport.Send(frame)
		})
		if errors.Is(sendErr, errSendTimedOut) {
			_ = ac.closeCapturedTransport(expected.transport)
		}
	}
	ac.output.unlockView()
	if sendErr == nil {
		prepared.commit()
	} else {
		prepared.abort()
	}
	d.retireGraphicsOutput(ac, state)
	return sendErr
}

func (d *Daemon) disableGraphicsOutput(ac *attachedClient) {
	if ac == nil || ac.graphicsOutput == nil {
		return
	}
	ac.terminalCapabilities.KittyGraphics = false
	state := ac.graphicsOutput
	if d == nil {
		ac.graphicsOutput = nil
		state.cleanup()
		return
	}
	d.retireGraphicsOutput(ac, state)
}

func graphicsOutputData(state *capturedRenderState, ac *attachedClient, reset bool) (*preparedGraphicsOutput, error) {
	return graphicsOutputDataWithDaemon(nil, state, ac, reset)
}

// composedGraphicsRecords is the pane-layout projection boundary. It reads
// only immutable captured snapshots and produces attachment-local keys and
// placements. In particular, it never joins or mutates the generic VT scenes.
func composedGraphicsRecords(state *capturedRenderState) (map[string]graphics.AssetView, map[string]graphicsOutputPlacement) {
	assets := make(map[string]graphics.AssetView)
	placements := make(map[string]graphicsOutputPlacement)
	if state == nil {
		return assets, placements
	}
	// Kitty placements are emitted after the ANSI frame. Any opaque overlay
	// would therefore be painted over by an old image unless its covered
	// fragments are temporarily removed. Modal overlays suppress the complete
	// scene; compact notices and floating panes contribute exact coverage
	// rectangles without mutating their source panes.
	if state.overlays.active() {
		return assets, placements
	}
	coverage := graphicsOpaqueCoverage(state)
	outerContent, hasOuterContent := graphicsOuterContentRect(state)

	for index, pane := range state.panes {
		if pane.graphics == nil || pane.placement.Collapsed || pane.placement.Content.Width <= 0 || pane.placement.Content.Height <= 0 {
			continue
		}
		var viewports []graphics.Viewport
		if hasOuterContent {
			viewports = paneGraphicsViewports(pane.placement.Content, pane.graphicsGeometry, coverage, outerContent)
		} else {
			viewports = paneGraphicsViewports(pane.placement.Content, pane.graphicsGeometry, coverage)
		}
		if len(viewports) == 0 {
			continue
		}
		transform := graphicsOutputTransform{originX: pane.placement.Content.X, originY: pane.placement.Content.Y + 1, sourceGeometry: pane.graphicsGeometry}
		source := graphicsSourceKey(state, pane.id, pane.stableID, false)
		paneAssets, panePlacements := graphicsSnapshotRecordsForSourceOrdered(pane.graphics, source, index, viewports...)
		mergeGraphicsRecords(assets, placements, paneAssets, panePlacements, transform)
	}

	if state.floating.visible && state.floating.pane.graphics != nil && state.floating.geometry.Inner.Width > 0 && state.floating.geometry.Inner.Height > 0 {
		inner := state.floating.geometry.Inner
		var viewports []graphics.Viewport
		if hasOuterContent {
			viewports = paneGraphicsViewports(inner, state.floating.pane.graphicsGeometry, nil, outerContent)
		} else {
			viewports = paneGraphicsViewports(inner, state.floating.pane.graphicsGeometry, nil)
		}
		if len(viewports) != 0 {
			transform := graphicsOutputTransform{originX: inner.X, originY: inner.Y + 1, sourceGeometry: state.floating.pane.graphicsGeometry}
			source := graphicsSourceKey(state, state.floating.pane.id, state.floating.pane.stableID, true)
			paneAssets, panePlacements := graphicsSnapshotRecordsForSourceOrdered(state.floating.pane.graphics, source, len(state.panes), viewports...)
			mergeGraphicsRecords(assets, placements, paneAssets, panePlacements, transform)
		}
	}
	return assets, placements
}

func graphicsSourceKey(state *capturedRenderState, paneID layout.PaneID, stableID domain.PaneStableID, floating bool) string {
	kind := "pane"
	if floating {
		kind = "floating"
	}
	if stableID == "" {
		stableID = domain.PaneStableID(paneID)
	}
	return fmt.Sprintf("%s:%q:%x:%q:%q:%q", kind, string(state.sessionID), state.incarnation, string(state.view.tabID), string(stableID), string(paneID))
}

func mergeGraphicsRecords(dstAssets map[string]graphics.AssetView, dstPlacements map[string]graphicsOutputPlacement, assets map[string]graphics.AssetView, placements map[string]graphicsOutputPlacement, transform graphicsOutputTransform) {
	for key, asset := range assets {
		dstAssets[key] = asset
	}
	for key, placement := range placements {
		placement.transform = transform
		placement.transformSet = true
		dstPlacements[key] = placement
	}
}

// paneGraphicsViewports returns pane content minus each opaque coverage
// rectangle. Each subtraction is ordered top, bottom, left, right, so a union
// of clipped fragments remains deterministic.
func graphicsOpaqueCoverage(state *capturedRenderState) []domain.Rect {
	if state == nil {
		return nil
	}
	coverage := make([]domain.Rect, 0, 1+len(state.overlays.notices))
	if state.floating.visible && state.floating.geometry.Bounds.Width > 0 && state.floating.geometry.Bounds.Height > 0 {
		coverage = append(coverage, state.floating.geometry.Bounds)
	}
	if len(state.overlays.notices) == 0 && state.overlays.noticeOverflow == 0 {
		return coverage
	}
	width, rows := state.layout.area.Width, state.layout.area.Height
	if state.window.Valid() {
		window := contentSize(state.window)
		width, rows = window.Cols, window.Rows
	}
	height := rows + tabChromeRows
	if width <= 0 || height <= 0 {
		for _, pane := range state.panes {
			if pane.placement.Content.X+pane.placement.Content.Width > width {
				width = pane.placement.Content.X + pane.placement.Content.Width
			}
			if pane.placement.Content.Y+pane.placement.Content.Height+tabChromeRows > height {
				height = pane.placement.Content.Y + pane.placement.Content.Height + tabChromeRows
			}
		}
	}
	if width <= 0 || height <= 0 {
		return coverage
	}
	styles := state.styles
	if styles == (themeui.Styles{}) {
		styles = fallbackChromeStyles
	}
	frame := renderer.NewFrame(width, height)
	for _, footprint := range composeCapturedNotices(state.overlays, frame, styles) {
		// Notices are laid out in complete-frame coordinates. The graphics
		// content origin is one row below that frame's top bar.
		footprint.Y--
		coverage = append(coverage, footprint)
	}
	return coverage
}

func graphicsOuterContentRect(state *capturedRenderState) (domain.Rect, bool) {
	if state == nil {
		return domain.Rect{}, false
	}
	if state.window.Valid() {
		content := contentSize(state.window)
		return domain.Rect{Width: content.Cols, Height: content.Rows}, content.Valid()
	}
	if state.layout.area.Width > 0 && state.layout.area.Height > 0 {
		return domain.Rect{Width: state.layout.area.Width, Height: state.layout.area.Height}, true
	}
	return domain.Rect{}, false
}

func paneGraphicsViewports(content domain.Rect, geometry domain.Geometry, coverage []domain.Rect, outerContent ...domain.Rect) []graphics.Viewport {
	full := domain.Rect{Width: content.Width, Height: content.Height}
	if content.Width <= 0 || content.Height <= 0 {
		return nil
	}
	regions := []domain.Rect{full}
	if len(outerContent) > 0 {
		visible, ok := intersectGraphicsDomainRect(content, outerContent[0])
		if !ok {
			return nil
		}
		visible.X -= content.X
		visible.Y -= content.Y
		regions[0] = visible
	}
	for _, opaque := range coverage {
		cover, ok := intersectGraphicsDomainRect(content, opaque)
		if !ok {
			continue
		}
		cover.X -= content.X
		cover.Y -= content.Y
		next := make([]domain.Rect, 0, len(regions)*4)
		for _, region := range regions {
			localCover, intersects := intersectGraphicsDomainRect(region, cover)
			if !intersects {
				next = append(next, region)
				continue
			}
			next = append(next, subtractGraphicsDomainRect(region, localCover)...)
		}
		regions = next
		if len(regions) == 0 {
			break
		}
	}
	viewports := make([]graphics.Viewport, 0, len(regions))
	for _, region := range regions {
		if region.Width <= 0 || region.Height <= 0 {
			continue
		}
		viewport, valid := graphicsOutputViewportRect(region, geometry)
		if valid {
			viewports = append(viewports, viewport)
		}
	}
	return viewports
}

func subtractGraphicsDomainRect(region, cover domain.Rect) []domain.Rect {
	return []domain.Rect{
		{X: region.X, Y: region.Y, Width: region.Width, Height: cover.Y - region.Y},
		{X: region.X, Y: cover.Y + cover.Height, Width: region.Width, Height: region.Y + region.Height - (cover.Y + cover.Height)},
		{X: region.X, Y: cover.Y, Width: cover.X - region.X, Height: cover.Height},
		{X: cover.X + cover.Width, Y: cover.Y, Width: region.X + region.Width - (cover.X + cover.Width), Height: cover.Height},
	}
}

func intersectGraphicsDomainRect(a, b domain.Rect) (domain.Rect, bool) {
	left, top := a.X, a.Y
	if b.X > left {
		left = b.X
	}
	if b.Y > top {
		top = b.Y
	}
	right, bottom := a.X+a.Width, a.Y+a.Height
	if b.X+b.Width < right {
		right = b.X + b.Width
	}
	if b.Y+b.Height < bottom {
		bottom = b.Y + b.Height
	}
	if right <= left || bottom <= top {
		return domain.Rect{}, false
	}
	return domain.Rect{X: left, Y: top, Width: right - left, Height: bottom - top}, true
}

func graphicsOutputDataWithDaemon(d *Daemon, state *capturedRenderState, ac *attachedClient, reset bool) (*preparedGraphicsOutput, error) {
	if ac == nil || ac.graphicsOutput == nil || state == nil || !ac.terminalCapabilities.SupportsKittyGraphics() {
		return nil, nil
	}
	assets, placements := composedGraphicsRecords(state)
	prepared, err := ac.graphicsOutput.prepareWithRecords(assets, placements, reset, graphicsOutputTransform{})
	if errors.Is(err, errGraphicsIDExhausted) {
		d.disableGraphicsOutput(ac)
		return nil, nil
	}
	if !errors.Is(err, errGraphicsOutputTooLarge) {
		return prepared, err
	}
	// Graphics are optional decoration. If an upload exceeds the bounded output
	// budget, suppress it and still return the bounded cleanup for objects that
	// were already proven to be owned by this attachment. ANSI composition is
	// prepared and emitted by the caller independently.
	prepared, err = ac.graphicsOutput.prepareWithRecords(nil, nil, reset, graphicsOutputTransform{})
	if errors.Is(err, errGraphicsIDExhausted) {
		d.disableGraphicsOutput(ac)
		return nil, nil
	}
	return prepared, err
}

func graphicsOutputError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("kitty graphics output: %w", err)
}
