//go:build !darwin && !linux

package fixturegen

// lockEnsure is advisory and only implemented where flock exists; elsewhere
// two concurrent ensures in one checkout are the operator's responsibility.
func lockEnsure(string) (func(), error) {
	return func() {}, nil
}
