package daemon

import "github.com/bnema/vev/pkg/vt"

func newTestHistory(rows int) *vt.History { return vt.NewHistory(vt.HistoryConfig{MaxRows: rows}) }
