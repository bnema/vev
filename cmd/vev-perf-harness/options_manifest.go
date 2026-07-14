package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func parseOptions(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("vev-perf-harness", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&o.vevBin, "vev-bin", "", "public vev binary")
	fs.StringVar(&o.manifest, "manifest", "", "manifest")
	fs.StringVar(&o.out, "out", "", "output directory")
	fs.StringVar(&o.scenario, "scenario", "", "one canonical scenario ID (after full manifest validation)")
	fs.DurationVar(&o.warmup, "warmup", 0, "warmup")
	fs.DurationVar(&o.duration, "duration", 0, "measurement duration")
	fs.IntVar(&o.repetitions, "repetitions", 0, "repetitions")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	seen := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	for _, name := range []string{"vev-bin", "manifest", "out", "warmup", "duration", "repetitions"} {
		if !seen[name] {
			return o, fmt.Errorf("--%s is required", name)
		}
	}
	if o.vevBin == "" || o.manifest == "" || o.out == "" {
		return o, errors.New("--vev-bin, --manifest, and --out are required")
	}
	if o.warmup < 0 {
		return o, errors.New("--warmup must not be negative")
	}
	if o.duration < minimumDuration {
		return o, fmt.Errorf("--duration must be at least %s", minimumDuration)
	}
	if o.repetitions < minimumRepetitions {
		return o, fmt.Errorf("--repetitions must be at least %d", minimumRepetitions)
	}
	return o, nil
}

// resolvePathOptions captures the harness cwd once, before role launchers
// assign their isolated working directories. All downstream filesystem access,
// manifests, and process environments consequently refer to the same paths.
func resolvePathOptions(o options) (options, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return o, fmt.Errorf("resolve harness working directory: %w", err)
	}
	absolute := func(path string) string {
		if filepath.IsAbs(path) {
			return filepath.Clean(path)
		}
		return filepath.Clean(filepath.Join(cwd, path))
	}
	o.vevBin = absolute(o.vevBin)
	o.manifest = absolute(o.manifest)
	o.out = absolute(o.out)
	return o, nil
}

func readManifest(path string) (manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return manifest{}, err
	}
	var m manifest
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return manifest{}, closeFile(f, err)
	}
	if err := f.Close(); err != nil {
		return manifest{}, err
	}
	return m, nil
}

func validateManifest(m manifest) error {
	if m.Schema != 1 {
		return errors.New("unsupported manifest schema")
	}
	tops := map[string]bool{}
	for _, t := range m.Topologies {
		if t.ID == "" {
			return errors.New("empty topology id")
		}
		if tops[t.ID] {
			return fmt.Errorf("duplicate topology id %q", t.ID)
		}
		if t.Geometry != "120x40" || t.RowsPerPane != 10000 {
			return fmt.Errorf("invalid topology %q", t.ID)
		}
		tops[t.ID] = true
	}
	wantT := []string{"1x4", "4x1", "4x4", "8x1"}
	for _, v := range wantT {
		if !tops[v] {
			return fmt.Errorf("missing canonical topology %s", v)
		}
	}
	works := map[string]bool{}
	for _, workload := range m.Workloads {
		if workload == "" {
			return errors.New("empty workload id")
		}
		if works[workload] {
			return fmt.Errorf("duplicate workload id %q", workload)
		}
		works[workload] = true
	}
	for _, v := range []string{"idle", "active_output", "all_output", "inactive_output", "interactive_flood", "copy_search", "resize_sweep", "snapshot_output_resize", "attach_restore_tab_switch"} {
		if !works[v] {
			return fmt.Errorf("missing canonical workload %s", v)
		}
	}
	trans := map[string]bool{}
	for _, t := range m.Transports {
		if t.ID == "" {
			return errors.New("empty transport id")
		}
		if trans[t.ID] {
			return fmt.Errorf("duplicate transport id %q", t.ID)
		}
		if err := validateTransportFixture(t); err != nil {
			return err
		}
		trans[t.ID] = true
	}
	for _, v := range []string{"local", "ssh_stdio", "udp_baseline", "udp_25ms", "udp_100ms", "udp_loss_0pct", "udp_loss_1pct"} {
		if !trans[v] {
			return fmt.Errorf("missing canonical transport %s", v)
		}
	}
	seen := map[string]bool{}
	covered := map[string]bool{}
	for _, s := range m.Scenarios {
		if s.ID == "" || seen[s.ID] {
			return fmt.Errorf("duplicate or empty scenario id %q", s.ID)
		}
		seen[s.ID] = true
		if !tops[s.Topology] || !works[s.Workload] || !trans[s.Transport] {
			return fmt.Errorf("scenario %s references unknown matrix entry", s.ID)
		}
		if s.InapplicableReason == "" && len(s.Roles) == 0 {
			return fmt.Errorf("scenario %s has no process roles", s.ID)
		}
		if s.InapplicableReason != "" && len(s.Roles) != 0 {
			return fmt.Errorf("inapplicable scenario %s must not launch roles", s.ID)
		}
		if s.InapplicableReason == "" && !equalRoleSet(s.Roles, requiredRoles(s.Transport)) {
			return fmt.Errorf("scenario %s roles do not route transport %s", s.ID, s.Transport)
		}
		covered[s.Topology+"\x00"+s.Workload+"\x00"+s.Transport] = true
	}
	for topologyID := range tops {
		for workload := range works {
			for transportID := range trans {
				key := topologyID + "\x00" + workload + "\x00" + transportID
				if !covered[key] {
					return fmt.Errorf("missing scenario or inapplicable reason for %s/%s/%s", topologyID, workload, transportID)
				}
			}
		}
	}
	return nil
}

// selectScenario is intentionally called only after validateManifest. A bounded
// local evidence run therefore cannot mask an incomplete canonical matrix.
func selectScenario(m manifest, id string) ([]scenario, error) {
	for _, s := range m.Scenarios {
		if s.ID != id {
			continue
		}
		if s.InapplicableReason != "" {
			return nil, fmt.Errorf("scenario %s is inapplicable: %s", id, s.InapplicableReason)
		}
		return []scenario{s}, nil
	}
	return nil, fmt.Errorf("unknown canonical scenario %s", id)
}

func requiredRoles(transportID string) []string {
	switch transportID {
	case "local":
		return []string{"daemon", "client"}
	case "ssh_stdio":
		return []string{"daemon", "client", "ssh_stdio_peer"}
	default:
		return []string{"daemon", "client", "udp_peer"}
	}
}

func equalRoleSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, role := range got {
		seen[role] = true
	}
	for _, role := range want {
		if !seen[role] {
			return false
		}
	}
	return true
}

func validateTransportFixture(t transport) error {
	want := map[string]transport{
		"local":         {ID: "local", Kind: "local"},
		"ssh_stdio":     {ID: "ssh_stdio", Kind: "ssh_stdio"},
		"udp_baseline":  {ID: "udp_baseline", Kind: "udp"},
		"udp_25ms":      {ID: "udp_25ms", Kind: "udp", RTTMS: 25},
		"udp_100ms":     {ID: "udp_100ms", Kind: "udp", RTTMS: 100},
		"udp_loss_0pct": {ID: "udp_loss_0pct", Kind: "udp", LossPercent: 0},
		"udp_loss_1pct": {ID: "udp_loss_1pct", Kind: "udp", LossPercent: 1},
	}
	w, ok := want[t.ID]
	if !ok || t.Kind != w.Kind || t.RTTMS != w.RTTMS || t.LossPercent != w.LossPercent {
		return fmt.Errorf("invalid canonical transport fixture %+v", t)
	}
	return nil
}
