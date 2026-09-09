package weaver

import (
    "bytes"
    "database/sql"
    "fmt"
    "math/rand/v2"
    "net"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "testing"
    "time"
)

func TestPar3OutputRestartE2E(t *testing.T) {
    bin, reference := os.Getenv("WEAVER_PAR3_E2E_BIN"), os.Getenv("WEAVER_PAR3_REFERENCE_BIN")
    if bin == "" || reference == "" { t.Skip("set native Weaver and official PAR3 reference binaries") }
    if !filepath.IsAbs(bin) || !filepath.IsAbs(reference) { t.Fatal("binaries must use absolute paths") }
    root, err := os.MkdirTemp("", "weaver-par3-output-restart-")
    if err != nil { t.Fatal(err) }
    t.Logf("preserved artifacts: %s", root)
    expected := map[string][]byte{}
    posted := map[string][]byte{}
    rng := rand.New(rand.NewPCG(29, 83))
    for _, name := range []string{"restored.bin", "downloaded.bin"} {
        data := make([]byte, 262144)
        for i := range data { data[i] = byte(rng.Uint32()) }
        expected[name] = data
        carrier := map[string][]byte{name: bytes.Clone(data)}
        options := []string{"-e1", "-s32768", "-c8"}
        prefix := "second."
        if name == "restored.bin" { options = append(options, "-D"); prefix = "first." }
        dir := filepath.Join(root, "sources", name)
        if err := os.MkdirAll(dir, 0755); err != nil { t.Fatal(err) }
        par3ReferenceParity(t, reference, dir, carrier, options)
        for file, body := range carrier {
            if name == "restored.bin" && (file == name || strings.Contains(file, ".vol")) { continue }
            if strings.HasSuffix(file, ".par3") { file = prefix + file }
            posted[file] = body
        }
    }
    posted["downloaded.bin"][100000] ^= 0x80
    nntp := startUnpackNNTP(t)
    port := nntp.listener.Addr().(*net.TCPAddr).Port
    url, firstLog, stop := startManagedUnpackWeaver(t, bin, root, "before.log", port,
        "RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
    api := provisionUnpackAPI(t, root, url)
    slug := "par3-output-restart"
    nzb := nntp.publishUnpack(slug, "clean", posted, nil)
    gate := &unpackGate{released: make(chan struct{})}
    release := sync.OnceFunc(func() { close(gate.released) })
    t.Cleanup(release)
    nntp.mu.Lock()
    for index, name := range unpackSortedNames(posted) {
        if !strings.HasPrefix(name, "second.") || !strings.Contains(name, ".vol") { continue }
        prefix := fmt.Sprintf("%s-%d-", slug, index)
        for id, article := range nntp.articles {
            if strings.HasPrefix(id, prefix) { article.gate = gate; nntp.articles[id] = article }
        }
    }
    nntp.mu.Unlock()
    job, err := api.submit(nzb, slug)
    if err != nil { t.Fatal(err) }
    db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(root, "weaver.db"))+"?mode=ro")
    if err != nil { t.Fatal(err) }
    db.SetMaxOpenConns(1)
    defer db.Close()
    restored := filepath.Join(root, "intermediate", slug, "restored.bin")
    ready := false
    deadline := time.Now().Add(30*time.Second)
    for time.Now().Before(deadline) {
        var records int
        err := db.QueryRow("SELECT COUNT(*) FROM active_repair_outputs WHERE job_id = ? AND filename = ?", job, "restored.bin").Scan(&records)
        actual, readErr := os.ReadFile(restored)
        if err == nil && records == 1 && readErr == nil && bytes.Equal(actual, expected["restored.bin"]) && gate.held.Load() > 0 {
            ready = true; break
        }
        if api.status(job) == "FAILED" { break }
        time.Sleep(20*time.Millisecond)
    }
    if !ready { t.Fatalf("never reached durable reconstructed-output wait: status=%s log=%s", api.status(job), firstLog) }
    stop()
    damaged := bytes.Clone(expected["restored.bin"])
    damaged[170000] ^= 0x80
    if err := os.WriteFile(restored, damaged, 0644); err != nil { t.Fatal(err) }
    release()
    url, secondLog, _ := startManagedUnpackWeaver(t, bin, root, "after.log", port,
        "RUST_LOG=info,weaver_server_core::pipeline::repair::par3=trace")
    api.url = url
    status := ""
    deadline = time.Now().Add(90*time.Second)
    for time.Now().Before(deadline) {
        status = api.status(job)
        if status == "COMPLETED" || status == "FAILED" { break }
        time.Sleep(50*time.Millisecond)
    }
    var history struct { HistoryItem *struct { Error *string; FailedBytes uint64; Health uint32 } }
    if err := api.query(`query($id:Int!) {historyItem(id:$id) {error failedBytes health}}`, map[string]any{"id":job}, &history); err != nil { t.Fatal(err) }
    if status != "COMPLETED" || history.HistoryItem == nil || history.HistoryItem.FailedBytes != 0 || history.HistoryItem.Health != 1000 {
        t.Fatalf("reconstructed-output restart status=%s history=%+v log=%s", status, history.HistoryItem, secondLog)
    }
    for name, want := range expected {
        actual, err := os.ReadFile(filepath.Join(root, "complete", slug, name))
        if err != nil || !bytes.Equal(actual, want) { t.Fatalf("wrong restored output %s: %v", name, err) }
    }
    var records int
    if err := db.QueryRow("SELECT COUNT(*) FROM active_repair_outputs WHERE job_id = ?", job).Scan(&records); err != nil || records != 0 {
        t.Fatalf("completed output manifest not retired: count=%d error=%v", records, err)
    }
}
