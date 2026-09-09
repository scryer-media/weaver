package weaver

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/fixturegen"
)

var unpackFormats = []string{"tar", "tar.gz", "tgz", "tar.bz2", "tar.xz", "gz", "bz2", "xz", "zst", "zstd", "br", "deflate", "split", "zip", "zip64", "zip64-stream"}

func unpackFixture(t *testing.T, dir, format string) (map[string][]byte, []byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4*1024*1024)
	rng := rand.New(rand.NewPCG(17, 41))
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	source := filepath.Join(dir, "payload.bin")
	if err := os.WriteFile(source, payload, 0644); err != nil {
		t.Fatal(err)
	}
	name := "payload.bin." + format
	if strings.HasPrefix(format, "tar") || format == "tgz" || strings.HasPrefix(format, "zip") {
		name = "archive." + format
	}
	if strings.HasPrefix(format, "zip64") {
		name = "archive.zip"
	}
	dest := filepath.Join(dir, name)
	var err error
	switch format {
	case "split":
		files := map[string][]byte{}
		for i := 0; i < 4; i++ {
			files[fmt.Sprintf("payload.bin.%03d", i+1)] = payload[i*1024*1024 : (i+1)*1024*1024]
		}
		return files, payload
	case "zip":
		err = fixturegen.WriteZip(dest, []fixturegen.Member{{Name: "payload.bin", Source: source}}, "")
	case "zip64", "zip64-stream":
		// Info-ZIP forced ZIP64 uses the same structures as >4 GiB members.
		// Filter mode writes a streaming ZIP64 descriptor and directory.
		cmd := exec.Command("zip", "-0", "-fz", dest, "payload.bin")
		cmd.Dir = dir
		if format == "zip64-stream" {
			cmd = exec.Command("zip", "-1")
			cmd.Stdin = bytes.NewReader(payload)
			var encoded []byte
			encoded, err = cmd.Output()
			if err == nil {
				err = os.WriteFile(dest, encoded, 0644)
			}
			// Info-ZIP calls a filter-mode member "-"; keep the expected name
			// explicit in the scenario instead of rewriting archive metadata.
		} else {
			_, err = cmd.CombinedOutput()
		}
	case "tar", "tar.gz", "tgz", "tar.bz2", "tar.xz":
		tarPath := filepath.Join(dir, "input.tar")
		err = fixturegen.WriteTar(tarPath, nil, []fixturegen.Member{{Name: "payload.bin", Source: source}})
		if err == nil {
			switch format {
			case "tar":
				err = os.Rename(tarPath, dest)
			case "tar.gz", "tgz":
				err = fixturegen.WriteGzip(dest, tarPath, "")
			case "tar.bz2":
				err = fixturegen.WriteBzip2(dest, tarPath)
			case "tar.xz":
				err = unpackXZ(dest, tarPath)
			}
		}
	case "gz":
		err = fixturegen.WriteGzip(dest, source, "")
	case "bz2":
		err = fixturegen.WriteBzip2(dest, source)
	case "xz":
		err = unpackXZ(dest, source)
	case "zst", "zstd":
		err = fixturegen.WriteZstd(dest, source)
	case "br":
		err = fixturegen.WriteBrotli(dest, source)
	case "deflate":
		err = fixturegen.WriteDeflate(dest, source)
	default:
		t.Fatalf("unknown format %s", format)
	}
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	return map[string][]byte{name: encoded}, payload
}

func unpackXZ(dest, source string) error {
	cmd := exec.Command("xz", "-0", "--threads=1", "--stdout", source)
	out, err := cmd.Output()
	if err != nil {
		return err
	}
	return os.WriteFile(dest, out, 0644)
}

func unpackParity(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	args := []string{"create", "-q", "-s65536", "-c20", filepath.Join(dir, "repair.par2")}
	for _, name := range unpackSortedNames(files) {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, files[name], 0644); err != nil {
			t.Fatal(err)
		}
		args = append(args, path)
	}
	cmd := exec.Command("par2", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("par2: %v: %s", err, out)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.par2"))
	if err != nil || len(paths) < 2 {
		t.Fatalf("missing recovery volumes: %v", err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files[filepath.Base(path)] = data
	}
}

func unpackSortedNames(files map[string][]byte) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *unpackNNTP) publishUnpack(slug, mode string, files map[string][]byte, gate *unpackGate) []byte {
	const segment = 64 * 1024
	var nzb bytes.Buffer
	nzb.WriteString(`<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">`)
	damaged := false
	s.mu.Lock()
	defer s.mu.Unlock()
	for fileIndex, name := range unpackSortedNames(files) {
		data := files[name]
		count := (len(data) + segment - 1) / segment
		fmt.Fprintf(&nzb, `<file poster="fixture" date="1" subject="%s"><groups><group>alt.test</group></groups><segments>`, xmlUnpackText(fmt.Sprintf(`"%s" yEnc (%d/%d)`, name, 1, count)))
		parity := strings.HasSuffix(name, ".par2") || strings.HasSuffix(name, ".par3")
		for index := 0; index < count; index++ {
			start, end := index*segment, min((index+1)*segment, len(data))
			body := bytes.Clone(data[start:end])
			id := fmt.Sprintf("%s-%d-%d@direct-unpack.test", slug, fileIndex, index)
			article := unpackArticle{}
			// Hold the middle, leaving ZIP's final directory article available.
			// One hole is enough to prevent completion without occupying every
			// connection and starving a seekable decoder's directory fetch.
			// One available part must finish to establish numbered topology.
			firstSplit := strings.HasSuffix(name, ".001") || (mode == "missing-first" && strings.HasSuffix(name, ".002"))
			if !parity && !firstSplit && index == count/2 {
				article.gate = gate
			}
			if !parity && !firstSplit && !damaged && index == count*3/4 {
				if mode == "missing" || mode == "joined-missing" {
					article.missing = true
				}
				if mode == "corrupt" {
					body[len(body)/2] ^= 0x80
				}
				damaged = true
			}
			article.body = unpackYenc(name, body, index+1, count, start+1, len(data), crc32.ChecksumIEEE(data))
			s.articles[id] = article
			fmt.Fprintf(&nzb, `<segment bytes="%d" number="%d">%s</segment>`, len(article.body), index+1, id)
		}
		nzb.WriteString(`</segments></file>`)
	}
	nzb.WriteString(`</nzb>`)
	return nzb.Bytes()
}
