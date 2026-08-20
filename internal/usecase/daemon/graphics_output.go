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

	"github.com/bnema/vev-vt/graphics"
	"github.com/bnema/vev/internal/domain"
)

// Graphics output is deliberately a small attachment-local backend. It emits
// only direct uploads and ordinary placements; Kitty frame/compose actions,
// multipane coordinate transforms, and remote graphics are outside this phase.
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
	digest := sha256.Sum256([]byte(key))
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
	prepared, err := s.prepareWithTransformOnce(snapshot, reset, transform, false, viewports...)
	if !errors.Is(err, errGraphicsIDExhausted) {
		return prepared, err
	}
	// A bounded attachment can safely recycle its own IDs only after deleting
	// the objects it owns. Keep this retry attachment-local: it never advances
	// into the next reserved namespace block.
	return s.prepareWithTransformOnce(snapshot, true, transform, true, viewports...)
}

func (s *graphicsOutputState) prepareWithTransformOnce(snapshot *graphics.Snapshot, reset bool, transform graphicsOutputTransform, resetIDs bool, viewports ...graphics.Viewport) (*preparedGraphicsOutput, error) {
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

	assets, placements := graphicsSnapshotRecords(snapshot, viewports...)
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
		if err := out.record(placementRecordWithTransform(asset.id, want, transform)); err != nil {
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
	assets := make(map[string]graphics.AssetView)
	placements := make(map[string]graphicsOutputPlacement)
	if snapshot == nil {
		return assets, placements
	}
	assetKeys := make(map[string]string)
	generation := snapshot.Generation()
	for _, asset := range snapshot.Assets() {
		key := graphicsAssetKey(generation, asset)
		assets[key] = asset
		assetKeys[asset.ID().String()] = key
	}

	placementViews := snapshot.Placements()
	if len(viewports) == 0 {
		for _, placement := range placementViews {
			addGraphicsOutputPlacement(placements, generation, assetKeys, placement.ID().String(), placement.AssetID().String(), placement.Source(), placement.Destination(), placement.Cells(), placement.Layer())
		}
		return assets, placements
	}

	byPlacementID := make(map[string]graphics.PlacementView, len(placementViews))
	for _, placement := range placementViews {
		byPlacementID[placement.ID().String()] = placement
	}
	cellFragments := make(map[string]graphics.VisibleFragment)
	if viewports[0].Cells.Valid() && !viewports[0].Cells.Empty() {
		for _, fragment := range snapshot.VisibleFragments(viewports[0]) {
			cellFragments[fragment.PlacementID.String()] = fragment
		}
	}
	for _, fragment := range snapshot.VisiblePixelFragments(viewports[0].Pixels) {
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
		addGraphicsOutputPlacement(placements, generation, assetKeys, fragment.PlacementID.String(), fragment.AssetID.String(), fragment.Source, fragment.Destination, fragment.Cells, placement.Layer())
	}
	return assets, placements
}

func addGraphicsOutputPlacement(dst map[string]graphicsOutputPlacement, generation uint64, assetKeys map[string]string, placementID, assetID string, source, destination graphics.PixelRect, cells graphics.CellRect, layer int64) {
	assetKey := assetKeys[assetID]
	if assetKey == "" {
		return
	}
	key := graphicsPlacementKeyParts(generation, placementID, assetKey, source, destination, cells, layer)
	dst[key] = graphicsOutputPlacement{asset: assetKey, source: source, dest: destination, cells: cells, layer: layer}
}

func graphicsAssetKey(generation uint64, asset graphics.AssetView) string {
	digest := sha256.Sum256(asset.Encoded())
	return fmt.Sprintf("g%d:%s:%d:%d:%x", generation, asset.ID().String(), asset.Width(), asset.Height(), digest)
}

func graphicsPlacementKey(generation uint64, placement graphics.PlacementView, assetKey string) string {
	return graphicsPlacementKeyParts(generation, placement.ID().String(), assetKey, placement.Source(), placement.Destination(), placement.Cells(), placement.Layer())
}

func graphicsPlacementKeyParts(generation uint64, placementID, assetKey string, source, dest graphics.PixelRect, cells graphics.CellRect, layer int64) string {
	return fmt.Sprintf("g%d:%s:%s:%v:%v:%v:%d", generation, placementID, assetKey, source, dest, cells, layer)
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
	return a.asset == b.asset && a.source == b.source && a.dest == b.dest && a.cells == b.cells && a.layer == b.layer
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
	if content.Width <= 0 || content.Height <= 0 {
		return graphics.Viewport{}, false
	}
	width, height := int64(content.Width), int64(content.Height)
	if geometry.PixelsKnown() && geometry.Cols > 0 && geometry.Rows > 0 {
		cellWidth := geometry.PixelWidth / geometry.Cols
		cellHeight := geometry.PixelHeight / geometry.Rows
		if cellWidth > 0 && cellHeight > 0 {
			width *= int64(cellWidth)
			height *= int64(cellHeight)
		}
	}
	return graphics.Viewport{
		Pixels: graphics.PixelRect{Width: width, Height: height},
		Cells:  graphics.CellRect{Width: int64(content.Width), Height: int64(content.Height)},
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
func (d *Daemon) cleanupGraphicsOutput(ac *attachedClient) {
	if d == nil || ac == nil || ac.graphicsOutput == nil {
		return
	}
	state := ac.graphicsOutput
	if ac.output == nil {
		d.retireGraphicsOutput(ac, state)
		return
	}
	ac.sendMu.Lock()
	prepared, err := state.prepare(nil, true)
	ac.sendMu.Unlock()
	if err != nil || prepared == nil {
		d.retireGraphicsOutput(ac, state)
		return
	}
	if len(prepared.data) == 0 {
		prepared.commit()
		d.retireGraphicsOutput(ac, state)
		return
	}
	prepared.markSendAttempted()
	if d.boundedSendOutputErr(ac, prepared.data) == nil {
		prepared.commit()
	} else {
		prepared.abort()
	}
	d.retireGraphicsOutput(ac, state)
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

func graphicsOutputDataWithDaemon(d *Daemon, state *capturedRenderState, ac *attachedClient, reset bool) (*preparedGraphicsOutput, error) {
	if ac == nil || ac.graphicsOutput == nil || state == nil || !ac.terminalCapabilities.SupportsKittyGraphics() {
		return nil, nil
	}
	// Phase 5 composes one pane only. A Kitty attachment still receives the
	// ordinary text composition for other layouts, but never receives graphics
	// whose coordinates would need a multipane transform.
	transform := graphicsOutputTransform{}
	snapshot := (*graphics.Snapshot)(nil)
	var viewport graphics.Viewport
	var hasViewport bool
	if len(state.panes) == 1 && !state.floating.visible {
		pane := state.panes[0]
		if !pane.placement.Collapsed && pane.placement.Content.Width > 0 && pane.placement.Content.Height > 0 {
			snapshot = pane.graphics
			transform = graphicsOutputTransform{
				originX:        pane.placement.Content.X,
				originY:        pane.placement.Content.Y + 1,
				sourceGeometry: pane.graphicsGeometry,
			}
			viewport, hasViewport = graphicsOutputViewport(pane.placement.Content, pane.graphicsGeometry)
		}
	}
	if hasViewport {
		prepared, err := ac.graphicsOutput.prepareWithTransform(snapshot, reset, transform, viewport)
		if errors.Is(err, errGraphicsIDExhausted) {
			d.disableGraphicsOutput(ac)
			return nil, nil
		}
		if !errors.Is(err, errGraphicsOutputTooLarge) {
			return prepared, err
		}
		prepared, err = ac.graphicsOutput.prepareWithTransform(nil, reset, transform, viewport)
		if errors.Is(err, errGraphicsIDExhausted) {
			d.disableGraphicsOutput(ac)
			return nil, nil
		}
		return prepared, err
	}
	prepared, err := ac.graphicsOutput.prepareWithTransform(snapshot, reset, transform)
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
	prepared, err = ac.graphicsOutput.prepareWithTransform(nil, reset, transform)
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
