// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

// Require every posted file, not just one completed member or a byte total.
// Root coverage also describes inputs retained by the graph in envelopes.
func completedDownloadCheckpoint(segments []nzbSegmentRef, rows []fileCoverageRow) bool {
	wanted := map[int]bool{}
	for _, segment := range segments {
		wanted[segment.FileIndex] = true
	}
	if len(wanted) == 0 {
		return false
	}
	for _, row := range rows {
		if row.Length != nil && *row.Length > 0 && row.Coverage.Healthy().Covers(0, *row.Length) {
			delete(wanted, row.FileIndex)
		}
	}
	return len(wanted) == 0
}
