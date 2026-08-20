package daemon

import (
	"bytes"
	"encoding/base64"
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
	nextImage     uint64
	nextPlace     uint64
	transform     graphicsOutputTransform
	transformSet  bool
}

type preparedGraphicsOutput struct {
	owner *graphicsOutputState
	state graphicsOutputState
	data  []byte
	valid bool
}

type graphicsOutputTransform struct {
	originX, originY int
	sourceGeometry   domain.Geometry
}

func newGraphicsOutputState() *graphicsOutputState {
	return &graphicsOutputState{
		assets:        make(map[string]graphicsOutputAsset),
		placements:    make(map[string]graphicsOutputPlacement),
		pendingImages: make(map[uint64]struct{}),
		pendingPlaces: make(map[uint64]struct{}),
		nextImage:     1,
		nextPlace:     1,
	}
}

func cloneGraphicsOutputState(in *graphicsOutputState) graphicsOutputState {
	out := graphicsOutputState{
		assets:        make(map[string]graphicsOutputAsset),
		placements:    make(map[string]graphicsOutputPlacement),
		pendingImages: make(map[uint64]struct{}),
		pendingPlaces: make(map[uint64]struct{}),
		nextImage:     1,
		nextPlace:     1,
	}
	if in == nil {
		return out
	}
	out.nextImage, out.nextPlace = in.nextImage, in.nextPlace
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
			id, ok = takeGraphicsID(&candidate.nextImage)
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
			id, ok := takeGraphicsID(&candidate.nextPlace)
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

func (p *preparedGraphicsOutput) abort() {
	if p == nil || !p.valid || p.owner == nil {
		return
	}
	// Send errors are ambiguous: the outer terminal may have consumed a prefix
	// of the record. Retain every speculative ID for an explicit delete on the
	// next reset before replaying the scene.
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
	p.valid = false
}

func graphicsSnapshotRecords(snapshot *graphics.Snapshot) (map[string]graphics.AssetView, map[string]graphicsOutputPlacement) {
	assets := make(map[string]graphics.AssetView)
	placements := make(map[string]graphicsOutputPlacement)
	if snapshot == nil {
		return assets, placements
	}
	for _, asset := range snapshot.Assets() {
		assets[asset.ID().String()] = asset
	}
	for _, placement := range snapshot.Placements() {
		key := placement.ID().String()
		placements[key] = graphicsOutputPlacement{
			asset:  placement.AssetID().String(),
			source: placement.Source(),
			dest:   placement.Destination(),
			cells:  placement.Cells(),
			layer:  placement.Layer(),
		}
	}
	return assets, placements
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
	if next == nil || *next == 0 {
		return 0, false
	}
	id := *next
	if id == ^uint64(0) {
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
	if len(state.panes) != 1 || state.floating.visible {
		return ac.graphicsOutput.prepare(nil, reset)
	}
	pane := state.panes[0]
	if pane.placement.Collapsed || pane.placement.Content.Width <= 0 || pane.placement.Content.Height <= 0 {
		return ac.graphicsOutput.prepare(nil, reset)
	}
	return ac.graphicsOutput.prepareWithTransform(pane.graphics, reset, graphicsOutputTransform{
		originX:        pane.placement.Content.X,
		originY:        pane.placement.Content.Y + 1,
		sourceGeometry: pane.graphicsGeometry,
	})
}

func graphicsOutputError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("kitty graphics output: %w", err)
}
