// Generate unmodified shared-file indexes with the pinned official reference.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func digest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }
func main() {
	if len(os.Args) != 2 {
		panic("usage: go run generate.go /absolute/path/to/par3")
	}
	reference, err := filepath.Abs(os.Args[1])
	check(err)
	binary, err := os.ReadFile(reference)
	check(err)
	scratch, err := os.MkdirTemp("", "weaver-par3-shared-fixtures-")
	check(err)
	defer os.RemoveAll(scratch)
	payload := make([]byte, 262144+123)
	rng := rand.New(rand.NewPCG(19, 41))
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	cases := []struct {
		name, codec, block string
		conflict           bool
	}{
		{"cauchy", "-e1", "-s32768", false},
		{"fft", "-e8", "-s16384", false},
		{"conflict", "-e8", "-s16384", true},
	}
	records := []map[string]any{}
	for _, spec := range cases {
		dir := filepath.Join(scratch, spec.name)
		check(os.Mkdir(dir, 0755))
		data := bytes.Clone(payload)
		if spec.conflict {
			data[0] ^= 0x80
		}
		check(os.WriteFile(filepath.Join(dir, "payload.bin"), data, 0644))
		args := []string{"create", spec.codec, spec.block, "-c8", "repair.par3", "payload.bin"}
		command := exec.Command(reference, args...)
		command.Dir = dir
		out, err := command.CombinedOutput()
		check(os.WriteFile(spec.name+"-creation.txt", out, 0644))
		check(err)
		index, err := os.ReadFile(filepath.Join(dir, "repair.par3"))
		check(err)
		check(os.WriteFile(spec.name+".par3", index, 0644))
		records = append(records, map[string]any{"index": spec.name + ".par3", "arguments": args,
			"payloadSHA256": digest(data), "indexSHA256": digest(index)})
	}
	provenance, err := json.MarshalIndent(map[string]any{
		"referenceRevision": "2971702e501f1350b1c7b9d11369af9157d6ed56",
		"referenceSHA256":   digest(binary), "sets": records,
	}, "", "  ")
	check(err)
	check(os.WriteFile("provenance.json", append(provenance, '\n'), 0644))
}
