// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//go:build !darwin && !linux

package fixturegen

// lockEnsure is advisory and only implemented where flock exists; elsewhere
// two concurrent ensures in one checkout are the operator's responsibility.
func lockEnsure(string) (func(), error) {
	return func() {}, nil
}
