package daemon

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"

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
	// namespace. It starts at a cryptographically random point and is shared
	// by image and placement IDs so neither kind can collide within an
	// attachment. A new attachment gets a fresh cursor.
	nextID       uint64
	transform    graphicsOutputTransform
	transformSet bool
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
)

func newGraphicsOutputState() *graphicsOutputState {
	return &graphicsOutputState{
		assets:        make(map[string]graphicsOutputAsset),
		placements:    make(map[string]graphicsOutputPlacement),
		pendingImages: make(map[uint64]struct{}),
		pendingPlaces: make(map[uint64]struct{}),
		nextID:        randomGraphicsIDCursor(),
	}
}

func randomGraphicsIDCursor() uint64 {
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("crypto/rand failed generating Kitty graphics ID cursor: " + err.Error())
	}
	// Leave enough room for the bounded scene to advance without wrapping.
	maxStart := maxKittyGraphicsID - maxGraphicsIDs
	return uint64(binary.BigEndian.Uint32(raw[:]))%maxStart + 1
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
	out.nextID = in.nextID
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

func (s *graphicsOutputState) prepareWithTransform(snapshot *graphics.Snapshot, reset bool, transform graphicsOutputTransform) (*preparedGraphicsOutput, error) {
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
	}

	assets, placements := graphicsSnapshotRecords(snapshot)
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
			id, ok = takeGraphicsID(&candidate.nextID)
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
			id, ok := takeGraphicsID(&candidate.nextID)
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
	if p != nil && p.valid && len(p.data) > 0 {
		p.sendAttempted = true
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

func graphicsSnapshotRecords(snapshot *graphics.Snapshot) (map[string]graphics.AssetView, map[string]graphicsOutputPlacement) {
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
	for _, placement := range snapshot.Placements() {
		assetKey := assetKeys[placement.AssetID().String()]
		key := graphicsPlacementKey(generation, placement, assetKey)
		placements[key] = graphicsOutputPlacement{
			asset:  assetKey,
			source: placement.Source(),
			dest:   placement.Destination(),
			cells:  placement.Cells(),
			layer:  placement.Layer(),
		}
	}
	return assets, placements
}

func graphicsAssetKey(generation uint64, asset graphics.AssetView) string {
	digest := sha256.Sum256(asset.Encoded())
	return fmt.Sprintf("g%d:%s:%d:%d:%x", generation, asset.ID().String(), asset.Width(), asset.Height(), digest)
}

func graphicsPlacementKey(generation uint64, placement graphics.PlacementView, assetKey string) string {
	source, dest, cells := placement.Source(), placement.Destination(), placement.Cells()
	return fmt.Sprintf("g%d:%s:%s:%v:%v:%v:%d", generation, placement.ID().String(), assetKey, source, dest, cells, placement.Layer())
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
	id := *next
	if id == maxKittyGraphicsID {
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

// cleanupGraphicsOutput removes the objects owned by a closing attachment
// before its transport is closed. Parked resumable attachments intentionally do
// not call this: their state remains attached to the parked link and the next
// epoch deletes and replays it as needed.
func (d *Daemon) cleanupGraphicsOutput(ac *attachedClient) {
	if d == nil || ac == nil || ac.graphicsOutput == nil || ac.output == nil {
		return
	}
	ac.sendMu.Lock()
	prepared, err := ac.graphicsOutput.prepare(nil, true)
	ac.sendMu.Unlock()
	if err != nil || prepared == nil {
		ac.graphicsOutput.cleanup()
		return
	}
	if len(prepared.data) == 0 {
		prepared.commit()
		ac.graphicsOutput.cleanup()
		return
	}
	prepared.markSendAttempted()
	if d.boundedSendOutputErr(ac, prepared.data) == nil {
		prepared.commit()
	} else {
		prepared.abort()
	}
	ac.graphicsOutput.cleanup()
}

func graphicsOutputData(state *capturedRenderState, ac *attachedClient, reset bool) (*preparedGraphicsOutput, error) {
	if ac == nil || ac.graphicsOutput == nil || state == nil {
		return nil, nil
	}
	// Phase 5 composes one pane only. A Kitty attachment still receives the
	// ordinary text composition for other layouts, but never receives graphics
	// whose coordinates would need a multipane transform.
	transform := graphicsOutputTransform{}
	snapshot := (*graphics.Snapshot)(nil)
	if len(state.panes) == 1 && !state.floating.visible {
		pane := state.panes[0]
		if !pane.placement.Collapsed && pane.placement.Content.Width > 0 && pane.placement.Content.Height > 0 {
			snapshot = pane.graphics
			transform = graphicsOutputTransform{
				originX:        pane.placement.Content.X,
				originY:        pane.placement.Content.Y + 1,
				sourceGeometry: pane.graphicsGeometry,
			}
		}
	}
	prepared, err := ac.graphicsOutput.prepareWithTransform(snapshot, reset, transform)
	if !errors.Is(err, errGraphicsOutputTooLarge) {
		return prepared, err
	}
	// Graphics are optional decoration. If an upload exceeds the bounded output
	// budget, suppress it and still return the bounded cleanup for objects that
	// were already proven to be owned by this attachment. ANSI composition is
	// prepared and emitted by the caller independently.
	return ac.graphicsOutput.prepareWithTransform(nil, reset, transform)
}

func graphicsOutputError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("kitty graphics output: %w", err)
}
