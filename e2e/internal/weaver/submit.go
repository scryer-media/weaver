package weaver

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// --- submit ---

func cmdSubmit(slug string) {
	// Find the NZB
	nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
	nzbData, err := os.ReadFile(nzbPath)
	if err != nil {
		log.Fatalf("read NZB %s: %v", nzbPath, err)
	}

	// Load scenario for metadata
	scenarioPath := filepath.Join(testdataDir(), slug, "scenario.json")
	scenario, err := loadScenario(filepath.Dir(scenarioPath))
	if err != nil {
		log.Fatalf("load scenario: %v", err)
	}

	weaverURL := defaultWeaverURL()
	prepareStandardTestRun(weaverURL, false)

	// Base64-encode the NZB
	nzbB64 := base64.StdEncoding.EncodeToString(nzbData)

	// Build GraphQL mutation
	query := `mutation($input: SubmitNzbInput!) {
		submitNzb(input: $input) {
			accepted
			item {
				id
				name
				state
			}
		}
	}`

	input := map[string]interface{}{
		"nzbBase64": nzbB64,
		"filename":  scenario.Title + ".nzb",
		"category":  scenario.Category,
	}
	if scenario.Password != "" {
		input["password"] = scenario.Password
	}

	payload := map[string]interface{}{
		"query":     query,
		"variables": map[string]interface{}{"input": input},
	}

	body, _ := json.Marshal(payload)
	log.Printf("submitting NZB to weaver (%s, %d bytes)...", slug, len(nzbData))

	resp, err := postGraphQL(weaverURL, body)
	if err != nil {
		log.Fatalf("submit to weaver: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	log.Printf("weaver response (%d): %s", resp.StatusCode, string(respBody))

	// Parse response to get job ID
	var gqlResp struct {
		Data struct {
			SubmitNzb struct {
				Accepted bool `json:"accepted"`
				Item     struct {
					ID    int    `json:"id"`
					Name  string `json:"name"`
					State string `json:"state"`
				} `json:"item"`
			} `json:"submitNzb"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(respBody, &gqlResp); err != nil {
		log.Fatalf("parse response: %v", err)
	}
	if len(gqlResp.Errors) > 0 {
		log.Fatalf("GraphQL error: %s", gqlResp.Errors[0].Message)
	}

	jobID := gqlResp.Data.SubmitNzb.Item.ID
	log.Printf("job created: id=%d name=%s state=%s accepted=%t", jobID, gqlResp.Data.SubmitNzb.Item.Name, gqlResp.Data.SubmitNzb.Item.State, gqlResp.Data.SubmitNzb.Accepted)

	// Poll job status until terminal
	log.Printf("polling job %d...", jobID)
	for {
		mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("job %d polling", jobID))
		job, err := fetchFacadeItemSnapshot(weaverURL, jobID)
		if err != nil {
			log.Printf("  poll error: %v", err)
			continue
		}
		if !job.Found {
			log.Printf("  poll warning: item %d not found yet", jobID)
			continue
		}

		log.Printf("  status=%s progress=%.1f%% health=%.1f%%",
			job.Status, job.ProgressPercent, float64(job.Health)/10)

		switch job.Status {
		case "COMPLETE":
			log.Printf("job %d completed successfully!", jobID)
			return
		case "FAILED":
			errMsg := job.Error
			if errMsg == "" {
				errMsg = "unknown"
			}
			log.Printf("job %d FAILED: %s", jobID, errMsg)
			if scenario.ExpectedOutcome == "health_failure" {
				log.Printf("(failure was expected for this scenario)")
				return
			}
			os.Exit(1)
		}
	}
}
