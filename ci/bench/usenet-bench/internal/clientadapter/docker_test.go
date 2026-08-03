package clientadapter

import "testing"

func TestParseContainerCgroup(t *testing.T) {
	for name, input := range map[string]string{
		"cgroup v2":     "0::/system.slice/docker-abc.scope\n",
		"perf event v1": "8:cpu,cpuacct:/docker/abc\n7:perf_event:/docker/abc\n",
	} {
		t.Run(name, func(t *testing.T) {
			cgroup, err := parseContainerCgroup(input)
			if err != nil {
				t.Fatal(err)
			}
			if cgroup == "" {
				t.Fatal("empty cgroup")
			}
		})
	}
	if _, err := parseContainerCgroup("0::/\n"); err == nil {
		t.Fatal("root cgroup must never be used for client telemetry")
	}
}
