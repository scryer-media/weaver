package benchmark

import "fmt"

// ExecutionTarget fixes both the operating-system family and the packaging
// boundary for a benchmark run. It is deliberately part of the plan rather
// than a machine-local switch: a result from a native host must never be
// aggregated as if it came from the Docker/Linux lane.
type ExecutionTarget string

const (
	DockerLinux   ExecutionTarget = "docker-linux"
	MacOSNative   ExecutionTarget = "macos-native"
	WindowsNative ExecutionTarget = "windows-native"
)

// TargetDescriptor is a human-readable description written into baseline
// documents and preflight reports. The ID is the only value persisted in a
// run, so target naming remains stable across hosts.
type TargetDescriptor struct {
	ID        ExecutionTarget `json:"id"`
	OS        string          `json:"os"`
	Packaging string          `json:"packaging"`
}

func DefaultExecutionTargets() []ExecutionTarget {
	return []ExecutionTarget{DockerLinux, MacOSNative, WindowsNative}
}

func DescribeExecutionTarget(target ExecutionTarget) (TargetDescriptor, error) {
	switch target {
	case DockerLinux:
		return TargetDescriptor{ID: target, OS: "linux", Packaging: "docker"}, nil
	case MacOSNative:
		return TargetDescriptor{ID: target, OS: "macos", Packaging: "native"}, nil
	case WindowsNative:
		return TargetDescriptor{ID: target, OS: "windows", Packaging: "native"}, nil
	default:
		return TargetDescriptor{}, fmt.Errorf("unsupported execution target %q", target)
	}
}

func validateExecutionTargets(targets []ExecutionTarget) error {
	if len(targets) == 0 {
		return fmt.Errorf("at least one execution target is required")
	}
	seen := map[ExecutionTarget]bool{}
	for _, target := range targets {
		if _, err := DescribeExecutionTarget(target); err != nil {
			return err
		}
		if seen[target] {
			return fmt.Errorf("execution targets repeat %q", target)
		}
		seen[target] = true
	}
	return nil
}
