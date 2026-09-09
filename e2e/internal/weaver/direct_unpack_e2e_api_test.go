package weaver

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type unpackAPI struct{ url, key string }

func provisionUnpackAPI(t *testing.T, root, url string) unpackAPI {
	t.Helper()
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		t.Fatal(err)
	}
	key := hex.EncodeToString(secret[:])
	if err := os.WriteFile(filepath.Join(root, "test-api-key"), []byte(key), 0600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(key))
	// The API listener starts after database initialization. Writing as soon as
	// api_keys exists races the remaining startup migrations on the same DB.
	deadline := time.Now().Add(30 * time.Second)
	client := &http.Client{Timeout: time.Second}
	for {
		resp, err := client.Get(url + "/graphql")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "weaver.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Provision only this test's newly created DB after startup has settled.
	for {
		_, err = db.Exec("INSERT INTO api_keys (name,key_hash,scope,created_at) VALUES (?,?,?,?)", "direct-unpack-e2e", hash[:], "admin", time.Now().UnixMilli())
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	api := unpackAPI{url: url, key: key}
	for {
		var version struct{ Version string }
		err = api.query("{ version }", nil, &version)
		if err == nil {
			return api
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (a unpackAPI) query(query string, variables any, out any) error {
	data, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, a.url+"/graphql", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.key)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GraphQL HTTP %d", resp.StatusCode)
	}
	var envelope struct {
		Data   json.RawMessage
		Errors []struct{ Message string }
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return err
	}
	if len(envelope.Errors) > 0 {
		return fmt.Errorf("GraphQL: %s", envelope.Errors[0].Message)
	}
	return json.Unmarshal(envelope.Data, out)
}

// Failed scenarios must not keep retrying or repairing while later cases run
// against the same isolated server. Preserve the original failure and artifacts.
func (a unpackAPI) cancelOnFailure(t *testing.T, job int) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		switch a.status(job) {
		case "COMPLETED", "FAILED", "CANCELLED":
			return
		}
		var result struct{ CancelJob bool }
		if err := a.query(`mutation($id:Int!) {cancelJob(id:$id)}`, map[string]any{"id": job}, &result); err != nil || !result.CancelJob {
			t.Errorf("cancel unfinished fixture job %d: cancelled=%v error=%v", job, result.CancelJob, err)
		}
	})
}

func (a unpackAPI) submit(nzb []byte, slug string) (int, error) {
	return a.submitWithPassword(nzb, slug, "")
}

func (a unpackAPI) submitWithPassword(nzb []byte, slug, password string) (int, error) {
	var result struct {
		SubmitNzb struct {
			Accepted bool
			Item     struct{ ID int }
		}
	}
	input := map[string]any{"nzbBase64": base64.StdEncoding.EncodeToString(nzb), "filename": slug + ".nzb"}
	if password != "" {
		input["password"] = password
	}
	err := a.query(`mutation($input: SubmitNzbInput!) {submitNzb(input:$input) {accepted item {id}}}`, map[string]any{"input": input}, &result)
	if err != nil {
		return 0, err
	}
	if !result.SubmitNzb.Accepted || result.SubmitNzb.Item.ID <= 0 {
		return 0, fmt.Errorf("NZB rejected: %s", slug)
	}
	return result.SubmitNzb.Item.ID, nil
}

func (a unpackAPI) status(job int) string {
	var result struct{ QueueItem, HistoryItem *struct{ State string } }
	if err := a.query(`query($id:Int!) {queueItem(id:$id) {state} historyItem(id:$id) {state}}`, map[string]any{"id": job}, &result); err != nil {
		return err.Error()
	}
	if result.HistoryItem != nil {
		return result.HistoryItem.State
	}
	if result.QueueItem != nil {
		return result.QueueItem.State
	}
	return "NOT_FOUND"
}
