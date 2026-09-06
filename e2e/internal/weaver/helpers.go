package weaver

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// --- helpers ---

func deleteArticles(slug string, pct int) error {
	return deleteArticlesAt(nntpHost(), nntpPort(), slug, pct)
}

func deleteArticlesAt(host, port, slug string, pct int) error {
	addr := host + ":" + port
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	// Read greeting
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	greeting, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read greeting: %w", err)
	}
	if !strings.HasPrefix(greeting, "200") {
		return fmt.Errorf("unexpected greeting: %s", strings.TrimSpace(greeting))
	}
	if err := authenticateNNTPConnection(conn, r, addr); err != nil {
		return err
	}

	// Send DELETE command
	prefix := fmt.Sprintf("e2e-%s", slug)
	cmd := fmt.Sprintf("DELETE %s %d\r\n", prefix, pct)
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return fmt.Errorf("write command: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	resp, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	resp = strings.TrimSpace(resp)
	if !strings.HasPrefix(resp, "290") {
		return fmt.Errorf("DELETE failed: %s", resp)
	}
	log.Printf("  %s", resp)

	conn.Write([]byte("QUIT\r\n"))
	return nil
}

func seededArticleIDs(slug string) ([]string, error) {
	nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
	nzbData, err := os.ReadFile(nzbPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read existing NZB %s: %w", nzbPath, err)
	}
	return extractMessageIDs(string(nzbData)), nil
}

func purgeSeededArticlesAt(host string, port string, slug string) error {
	messageIDs, err := seededArticleIDs(slug)
	if err != nil {
		return err
	}
	return deleteArticleIDsAt(host, port, messageIDs)
}

func purgeSeededArticles(slug string) error {
	if err := purgeSeededArticlesAt(nntpHost(), nntpPort(), slug); err != nil {
		return err
	}
	if backupNntpRunning() {
		if err := purgeSeededArticlesAt(nntpHost(), backupNntpPort(), slug); err != nil {
			return err
		}
	}
	return nil
}

func waitForTCP(addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		mustSleepWithSuspendDetection(time.Second, fmt.Sprintf("waiting for TCP %s", addr))
	}
	log.Fatalf("timeout waiting for %s", addr)
}

type facadeItemSnapshot struct {
	Found                           bool
	InQueue                         bool
	Status                          string
	ProgressPercent                 float64
	Health                          int
	Error                           string
	TotalBytes                      uint64
	DownloadedBytes                 uint64
	OptionalRecoveryBytes           uint64
	OptionalRecoveryDownloadedBytes uint64
	FailedBytes                     uint64
}

func normalizeFacadeState(state string) string {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "COMPLETED":
		return "COMPLETE"
	case "FAILED":
		return "FAILED"
	default:
		return strings.ToUpper(strings.TrimSpace(state))
	}
}

func facadeTerminalStatus(state string) bool {
	switch normalizeFacadeState(state) {
	case "COMPLETE", "FAILED":
		return true
	default:
		return false
	}
}

func fetchFacadeItemSnapshot(weaverURL string, jobID int) (facadeItemSnapshot, error) {
	var result facadeItemSnapshot
	payload, _ := json.Marshal(map[string]interface{}{
		"query": `query($id: Int!) {
			queueItem(id: $id) {
				id
				state
				progressPercent
				health
				error
				totalBytes
				downloadedBytes
				optionalRecoveryBytes
				optionalRecoveryDownloadedBytes
				failedBytes
			}
			historyItem(id: $id) {
				id
				state
				progressPercent
				health
				error
				totalBytes
				downloadedBytes
				failedBytes
			}
		}`,
		"variables": map[string]interface{}{"id": jobID},
	})
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := postGraphQLWithClient(client, weaverURL, payload)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return result, fmt.Errorf("facade snapshot returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var gqlResp struct {
		Data struct {
			QueueItem *struct {
				State                           string  `json:"state"`
				ProgressPercent                 float64 `json:"progressPercent"`
				Health                          int     `json:"health"`
				Error                           *string `json:"error"`
				TotalBytes                      uint64  `json:"totalBytes"`
				DownloadedBytes                 uint64  `json:"downloadedBytes"`
				OptionalRecoveryBytes           uint64  `json:"optionalRecoveryBytes"`
				OptionalRecoveryDownloadedBytes uint64  `json:"optionalRecoveryDownloadedBytes"`
				FailedBytes                     uint64  `json:"failedBytes"`
			} `json:"queueItem"`
			HistoryItem *struct {
				State           string  `json:"state"`
				ProgressPercent float64 `json:"progressPercent"`
				Health          int     `json:"health"`
				Error           *string `json:"error"`
				TotalBytes      uint64  `json:"totalBytes"`
				DownloadedBytes uint64  `json:"downloadedBytes"`
				FailedBytes     uint64  `json:"failedBytes"`
			} `json:"historyItem"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return result, err
	}
	if len(gqlResp.Errors) > 0 {
		return result, fmt.Errorf("gql: %s", gqlResp.Errors[0].Message)
	}

	if item := gqlResp.Data.QueueItem; item != nil {
		result.Found = true
		result.InQueue = true
		result.Status = normalizeFacadeState(item.State)
		result.ProgressPercent = item.ProgressPercent
		result.Health = item.Health
		result.TotalBytes = item.TotalBytes
		result.DownloadedBytes = item.DownloadedBytes
		result.OptionalRecoveryBytes = item.OptionalRecoveryBytes
		result.OptionalRecoveryDownloadedBytes = item.OptionalRecoveryDownloadedBytes
		result.FailedBytes = item.FailedBytes
		if item.Error != nil {
			result.Error = *item.Error
		}
		return result, nil
	}
	if item := gqlResp.Data.HistoryItem; item != nil {
		result.Found = true
		result.Status = normalizeFacadeState(item.State)
		result.ProgressPercent = item.ProgressPercent
		result.Health = item.Health
		result.TotalBytes = item.TotalBytes
		result.DownloadedBytes = item.DownloadedBytes
		result.FailedBytes = item.FailedBytes
		if item.Error != nil {
			result.Error = *item.Error
		}
		return result, nil
	}

	return result, nil
}

func cancelJobGraphQL(weaverURL string, jobID int) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"query":     `mutation($id: Int!) { cancelQueueItem(id: $id) { success message } }`,
		"variables": map[string]interface{}{"id": jobID},
	})
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := postGraphQLWithClient(client, weaverURL, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cancel job %d returned %d: %s", jobID, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var gqlResp struct {
		Data struct {
			CancelQueueItem struct {
				Success bool    `json:"success"`
				Message *string `json:"message"`
			} `json:"cancelQueueItem"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return err
	}
	if len(gqlResp.Errors) > 0 {
		return fmt.Errorf("cancel job %d gql: %s", jobID, gqlResp.Errors[0].Message)
	}
	if !gqlResp.Data.CancelQueueItem.Success {
		message := ""
		if gqlResp.Data.CancelQueueItem.Message != nil {
			message = *gqlResp.Data.CancelQueueItem.Message
		}
		return fmt.Errorf("cancel job %d did not succeed: %s", jobID, message)
	}
	return nil
}

const (
	weaverCancelSettleTimeout      = 15 * time.Second
	weaverCancelSettlePollInterval = 500 * time.Millisecond
)

func waitForJobCancelSettledGraphQL(weaverURL string, jobID int, timeout, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = weaverCancelSettlePollInterval
	}
	deadline := time.Now().Add(timeout)
	lastState := "not checked"

	for {
		snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobID)
		if err != nil {
			lastState = err.Error()
		} else if !snapshot.Found {
			return nil
		} else {
			status := normalizeFacadeState(snapshot.Status)
			lastState = fmt.Sprintf(
				"found=%t in_queue=%t status=%s progress=%.2f health=%d failed_bytes=%d error=%q",
				snapshot.Found,
				snapshot.InQueue,
				status,
				snapshot.ProgressPercent,
				snapshot.Health,
				snapshot.FailedBytes,
				snapshot.Error,
			)
			if cancelSettledState(status) {
				return nil
			}
		}

		if timeout <= 0 || !time.Now().Before(deadline) {
			return fmt.Errorf("job %d did not settle after cancel within %s: %s", jobID, timeout, lastState)
		}
		time.Sleep(pollInterval)
	}
}

func cancelSettledState(state string) bool {
	switch normalizeFacadeState(state) {
	case "COMPLETE", "FAILED", "CANCELLED", "CANCELED":
		return true
	default:
		return false
	}
}

func describeJobsGraphQL(weaverURL string) string {
	jobs, err := listJobsGraphQL(weaverURL)
	if err != nil {
		return fmt.Sprintf("list jobs failed: %v", err)
	}
	if len(jobs) == 0 {
		return "no queue/history jobs"
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].ID == jobs[j].ID {
			return jobs[i].Status < jobs[j].Status
		}
		return jobs[i].ID < jobs[j].ID
	})
	parts := make([]string, 0, len(jobs))
	for _, job := range jobs {
		parts = append(parts, fmt.Sprintf("%d=%s", job.ID, job.Status))
	}
	return strings.Join(parts, ", ")
}

func localWeaverLogTail(maxLines int) string {
	if maxLines <= 0 {
		maxLines = 40
	}
	path := localWeaverLogPath()
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("read %s: %v", path, err)
	}
	text := strings.TrimRight(string(data), "\n")
	if text == "" {
		return fmt.Sprintf("%s is empty", path)
	}
	lines := strings.Split(text, "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return strings.Join(lines, "\n")
}

func deleteAllHistoryGraphQL(weaverURL string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	listPayload, _ := json.Marshal(map[string]interface{}{
		"query": `query {
			historyItems(first: 1000) {
				id
			}
		}`,
	})
	resp, err := postGraphQLWithClient(client, weaverURL, listPayload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("list history returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var listResp struct {
		Data struct {
			HistoryItems []struct {
				ID int `json:"id"`
			} `json:"historyItems"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		return err
	}
	if len(listResp.Errors) > 0 {
		return fmt.Errorf("list history gql: %s", listResp.Errors[0].Message)
	}
	if len(listResp.Data.HistoryItems) == 0 {
		return nil
	}
	ids := make([]int, 0, len(listResp.Data.HistoryItems))
	for _, item := range listResp.Data.HistoryItems {
		ids = append(ids, item.ID)
	}

	deletePayload, _ := json.Marshal(map[string]interface{}{
		"query": `mutation($ids: [Int!]!) {
			removeHistoryItems(ids: $ids, deleteFiles: true) {
				success
				removedIds
			}
		}`,
		"variables": map[string]interface{}{"ids": ids},
	})
	deleteResp, err := postGraphQLWithClient(client, weaverURL, deletePayload)
	if err != nil {
		return err
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != 200 {
		body, _ := io.ReadAll(deleteResp.Body)
		return fmt.Errorf("delete history returned %d: %s", deleteResp.StatusCode, strings.TrimSpace(string(body)))
	}
	var gqlResp struct {
		Data struct {
			RemoveHistoryItems struct {
				Success bool `json:"success"`
			} `json:"removeHistoryItems"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(deleteResp.Body).Decode(&gqlResp); err != nil {
		return err
	}
	if len(gqlResp.Errors) > 0 {
		return fmt.Errorf("delete history gql: %s", gqlResp.Errors[0].Message)
	}
	if !gqlResp.Data.RemoveHistoryItems.Success {
		return fmt.Errorf("delete history did not succeed")
	}
	return nil
}

func listJobsGraphQL(weaverURL string) ([]struct {
	ID     int    `json:"id"`
	Status string `json:"status"`
}, error) {
	payload, _ := json.Marshal(map[string]interface{}{
		"query": `query {
			queueItems(first: 1000) { id state }
			historyItems(first: 1000) { id state }
		}`,
	})
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := postGraphQLWithClient(client, weaverURL, payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list jobs returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var gqlResp struct {
		Data struct {
			QueueItems []struct {
				ID    int    `json:"id"`
				State string `json:"state"`
			} `json:"queueItems"`
			HistoryItems []struct {
				ID    int    `json:"id"`
				State string `json:"state"`
			} `json:"historyItems"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return nil, fmt.Errorf("decode jobs response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return nil, fmt.Errorf("list jobs GraphQL error: %s", gqlResp.Errors[0].Message)
	}
	jobs := make([]struct {
		ID     int    `json:"id"`
		Status string `json:"status"`
	}, 0, len(gqlResp.Data.QueueItems)+len(gqlResp.Data.HistoryItems))
	for _, item := range gqlResp.Data.QueueItems {
		jobs = append(jobs, struct {
			ID     int    `json:"id"`
			Status string `json:"status"`
		}{ID: item.ID, Status: normalizeFacadeState(item.State)})
	}
	for _, item := range gqlResp.Data.HistoryItems {
		jobs = append(jobs, struct {
			ID     int    `json:"id"`
			Status string `json:"status"`
		}{ID: item.ID, Status: normalizeFacadeState(item.State)})
	}
	return jobs, nil
}

func prepareStandardTestRun(weaverURL string, clearHistory bool) {
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)

	if err := ensureNntpChaosOff(); err != nil {
		log.Fatalf("reset NNTP chaos before test run: %v", err)
	}
	if !clearHistory {
		return
	}

	jobs, err := listJobsGraphQL(weaverURL)
	if err != nil {
		log.Fatalf("list weaver jobs before test run: %v", err)
	}
	for _, job := range jobs {
		if job.Status == "COMPLETE" || job.Status == "FAILED" {
			continue
		}
		if err := cancelJobGraphQL(weaverURL, job.ID); err != nil {
			log.Printf("warning: cancel stale job %d (%s): %v", job.ID, job.Status, err)
		}
	}
	if err := deleteAllHistoryGraphQL(weaverURL); err != nil {
		log.Fatalf("clear weaver history before test run: %v", err)
	}
	time.Sleep(2 * time.Second)
}

func tailFileLines(path string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return []string{fmt.Sprintf("failed to read %s: %v", path, err)}
	}
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	var trimmed []string
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			trimmed = append(trimmed, line)
		}
	}
	if len(trimmed) > limit {
		trimmed = trimmed[len(trimmed)-limit:]
	}
	return trimmed
}

func writeChaosRoundArtifacts(
	root string,
	roundNumber int,
	name string,
	config string,
	jobs []chaosRoundJobArtifact,
	weaverURL string,
) {
	type roundArtifact struct {
		Round         int                     `json:"round"`
		Name          string                  `json:"name"`
		Config        string                  `json:"config"`
		RecordedAt    string                  `json:"recorded_at"`
		Jobs          []chaosRoundJobArtifact `json:"jobs"`
		CurrentWeaver []struct {
			ID     int    `json:"id"`
			Status string `json:"status"`
		} `json:"current_weaver_jobs,omitempty"`
		WeaverLogTail []string `json:"weaver_log_tail,omitempty"`
	}

	currentJobs, err := listJobsGraphQL(weaverURL)
	if err != nil {
		log.Printf("warning: list jobs for NNTP chaos round %d artifact: %v", roundNumber, err)
	}
	enrichedJobs := append([]chaosRoundJobArtifact(nil), jobs...)
	for i := range enrichedJobs {
		if enrichedJobs[i].JobID <= 0 {
			continue
		}
		snapshot, snapshotErr := fetchFacadeItemSnapshot(weaverURL, enrichedJobs[i].JobID)
		if snapshotErr != nil {
			enrichedJobs[i].Error = fmt.Sprintf("snapshot error: %v", snapshotErr)
			continue
		}
		enrichedJobs[i].Found = snapshot.Found
		enrichedJobs[i].ProgressPercent = snapshot.ProgressPercent
		enrichedJobs[i].Health = snapshot.Health
		if snapshot.Error != "" {
			enrichedJobs[i].Error = snapshot.Error
		}
		if snapshot.Found && facadeTerminalStatus(snapshot.Status) {
			enrichedJobs[i].Status = snapshot.Status
		}
	}
	artifact := roundArtifact{
		Round:         roundNumber,
		Name:          name,
		Config:        config,
		RecordedAt:    time.Now().Format(time.RFC3339Nano),
		Jobs:          enrichedJobs,
		CurrentWeaver: currentJobs,
		WeaverLogTail: tailFileLines(localWeaverLogPath(), 200),
	}
	data, marshalErr := json.MarshalIndent(artifact, "", "  ")
	if marshalErr != nil {
		log.Printf("warning: marshal NNTP chaos round %d artifact: %v", roundNumber, marshalErr)
		return
	}
	path := filepath.Join(root, fmt.Sprintf("round-%02d.json", roundNumber))
	if writeErr := os.WriteFile(path, data, 0o644); writeErr != nil {
		log.Printf("warning: write NNTP chaos round %d artifact: %v", roundNumber, writeErr)
	}
}

func waitForGraphQL(url string, timeout time.Duration) {
	client := weaverHTTPClient(url, 3*time.Second)
	body := []byte(`{"query":"{ version }"}`)
	deadline := time.Now().Add(timeout)
	lastFailure := "not attempted"
	for time.Now().Before(deadline) {
		if err := refreshWeaverBrowserSession(client, url); err != nil {
			lastFailure = "load UI: " + err.Error()
			mustSleepWithSuspendDetection(time.Second, fmt.Sprintf("waiting for GraphQL %s", url))
			continue
		}
		resp, err := postGraphQLWithClient(client, url, body)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		lastFailure = describeGraphQLAttempt(resp, err)
		mustSleepWithSuspendDetection(time.Second, fmt.Sprintf("waiting for GraphQL %s", url))
	}
	log.Fatalf("timeout waiting for %s (last failure: %s)", url, lastFailure)
}

func describeGraphQLAttempt(resp *http.Response, err error) string {
	if err != nil {
		return err.Error()
	}
	if resp == nil {
		return "no response"
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	snippet := strings.TrimSpace(string(body))
	if snippet == "" {
		return fmt.Sprintf("status %d", resp.StatusCode)
	}
	return fmt.Sprintf("status %d: %s", resp.StatusCode, snippet)
}

func waitForHTTP(url string, timeout time.Duration) {
	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		mustSleepWithSuspendDetection(time.Second, fmt.Sprintf("waiting for HTTP %s", url))
	}
	log.Fatalf("timeout waiting for %s", url)
}

func defaultWeaverURL() string {
	if value := strings.TrimSpace(os.Getenv("WEAVER_URL")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return fmt.Sprintf("http://localhost:%s", os.Getenv("E2E_WEAVER_PORT"))
}

func localWeaverPort() string {
	if value := strings.TrimSpace(os.Getenv("WEAVER_PORT")); value != "" {
		return value
	}
	ensureRuntimePortEnv()
	return os.Getenv("E2E_LOCAL_WEAVER_PORT")
}

func graphqlURL(weaverURL string) string {
	if strings.HasSuffix(weaverURL, "/graphql") {
		return weaverURL
	}
	return strings.TrimRight(weaverURL, "/") + "/graphql"
}

func weaverBaseURL(weaverURL string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(weaverURL), "/")
	return strings.TrimSuffix(trimmed, "/graphql")
}

func weaverHTTPClient(weaverURL string, timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &http.Client{
		Timeout: timeout,
		Jar:     weaverCookieJar(weaverURL),
	}
}

func weaverCookieJar(weaverURL string) http.CookieJar {
	baseURL := weaverBaseURL(weaverURL)
	if jar, ok := weaverCookieJars.Load(baseURL); ok {
		if typed, ok := jar.(http.CookieJar); ok {
			return typed
		}
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		log.Fatalf("create Weaver cookie jar: %v", err)
	}
	actual, _ := weaverCookieJars.LoadOrStore(baseURL, jar)
	return actual.(http.CookieJar)
}

func ensureClientCookieJar(client *http.Client) error {
	if client.Jar != nil {
		return nil
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return err
	}
	client.Jar = jar
	return nil
}

func refreshWeaverBrowserSession(client *http.Client, weaverURL string) error {
	if client == nil {
		return fmt.Errorf("nil http client")
	}
	if err := ensureClientCookieJar(client); err != nil {
		return fmt.Errorf("create cookie jar: %w", err)
	}
	resp, err := client.Get(weaverBaseURL(weaverURL))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("load Weaver UI returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func doGraphQLPost(client *http.Client, weaverURL string, payload []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, graphqlURL(weaverURL), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}

func postGraphQL(weaverURL string, payload []byte) (*http.Response, error) {
	return postGraphQLWithClient(weaverHTTPClient(weaverURL, 10*time.Second), weaverURL, payload)
}

func postGraphQLWithClient(client *http.Client, weaverURL string, payload []byte) (*http.Response, error) {
	if client == nil {
		client = weaverHTTPClient(weaverURL, 10*time.Second)
	}

	resp, err := doGraphQLPost(client, weaverURL, payload)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if err := refreshWeaverBrowserSession(client, weaverURL); err != nil {
		return nil, fmt.Errorf("graphql unauthorized and failed to load Weaver browser session: %w", err)
	}
	return doGraphQLPost(client, weaverURL, payload)
}
