package clientadapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

type productAPI interface {
	waitReady(context.Context) (string, error)
	queue(context.Context, string, string, queueOptions) (string, error)
	waitComplete(context.Context, string, time.Duration) (time.Time, error)
	observe(context.Context, []string) (map[string]jobObservation, error)
}

type queueOptions struct {
	submissionName string
	forceAccept    bool
}

func (options queueOptions) filename(nzbPath string) string {
	if strings.TrimSpace(options.submissionName) != "" {
		return options.submissionName
	}
	return filepath.Base(nzbPath)
}

type jobObservationState uint8

const (
	jobUnknown jobObservationState = iota
	jobQueued
	jobActive
	jobComplete
	jobFailed
)

type jobObservation struct {
	state  jobObservationState
	status string
}

func classifyLiveStatus(status string) jobObservation {
	normalized := strings.ToUpper(strings.TrimSpace(status))
	switch normalized {
	case "":
		return jobObservation{state: jobUnknown}
	case "QUEUED", "PAUSED":
		return jobObservation{state: jobQueued, status: status}
	default:
		return jobObservation{state: jobActive, status: status}
	}
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

// API is the product-neutral public-control-plane adapter shared by the
// Docker and native launchers. It deliberately exposes only readiness, NZB
// submission, and terminal completion; no client-specific fast path leaks
// into the benchmark contract.
type API struct {
	product productAPI
}

func NewAPI(client benchmark.Client, endpoint string) (*API, error) {
	var product productAPI
	switch client {
	case benchmark.SABnzbd:
		product = &sabAPI{baseURL: endpoint, client: &http.Client{Timeout: 30 * time.Second}}
	case benchmark.NZBGet:
		product = &nzbgetAPI{baseURL: endpoint, client: &http.Client{Timeout: 30 * time.Second}}
	case benchmark.Weaver:
		jar, err := cookiejar.New(nil)
		if err != nil {
			return nil, fmt.Errorf("create Weaver session cookie jar: %w", err)
		}
		product = &weaverAPI{baseURL: endpoint, client: &http.Client{Timeout: 30 * time.Second, Jar: jar}}
	default:
		return nil, fmt.Errorf("unsupported client %q", client)
	}
	return &API{product: product}, nil
}

func (api *API) WaitReady(ctx context.Context) (string, error) {
	return api.product.waitReady(ctx)
}

func (api *API) Queue(ctx context.Context, nzbPath, archivePassword string) (string, error) {
	return api.product.queue(ctx, nzbPath, archivePassword, queueOptions{})
}

// QueueTiming bounds a public API submission with timestamps captured by the
// launcher immediately before the request and after the accepted response.
type QueueTiming struct {
	JobID               string
	SubmissionStartedAt time.Time
	AcceptedAt          time.Time
}

// QueueWithTiming queues one NZB and records the actual public API submission
// interval. It is intended for adapters that must preserve queue-acceptance
// timing rather than infer it from a later poll.
func (api *API) QueueWithTiming(ctx context.Context, nzbPath, archivePassword string) (QueueTiming, error) {
	timing := QueueTiming{SubmissionStartedAt: time.Now()}
	jobID, err := api.Queue(ctx, nzbPath, archivePassword)
	timing.AcceptedAt = time.Now()
	if err != nil {
		return QueueTiming{}, err
	}
	timing.JobID = jobID
	return timing, nil
}

// TerminalObservation records the honest interval in which terminal status
// first became observable through the public client API.
type TerminalObservation struct {
	LowerBound time.Time
	ObservedAt time.Time
}

// WaitCompleteWithObservation polls the public API and retains the previous
// confirmed non-terminal observation as the terminal lower bound. Callers can
// therefore report uncertainty without substituting a nominal poll interval.
func (api *API) WaitCompleteWithObservation(ctx context.Context, jobID string, interval time.Duration, acceptedAt time.Time) (TerminalObservation, error) {
	lowerBound := acceptedAt
	for {
		observations, err := api.product.observe(ctx, []string{jobID})
		observedAt := time.Now()
		if err != nil {
			return TerminalObservation{}, err
		}
		observation, found := observations[jobID]
		if found {
			switch observation.state {
			case jobComplete:
				return TerminalObservation{LowerBound: lowerBound, ObservedAt: observedAt}, nil
			case jobFailed:
				return TerminalObservation{}, fmt.Errorf("client job %s terminal status %q", jobID, observation.status)
			case jobQueued, jobActive:
				lowerBound = observedAt
			}
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return TerminalObservation{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (api *API) WaitComplete(ctx context.Context, jobID string, interval time.Duration) (time.Time, error) {
	return api.product.waitComplete(ctx, jobID, interval)
}

func newProductAPI(cfg Config, endpoint string) (productAPI, error) {
	api, err := NewAPI(cfg.Client, endpoint)
	if err != nil {
		return nil, err
	}
	return api.product, nil
}

type sabAPI struct {
	baseURL string
	client  *http.Client
}

func (api *sabAPI) waitReady(ctx context.Context) (string, error) {
	var response struct {
		Version string `json:"version"`
	}
	if err := api.get(ctx, "version", nil, &response); err != nil {
		return "", err
	}
	if strings.TrimSpace(response.Version) == "" {
		return "", fmt.Errorf("SABnzbd version response was empty")
	}
	return response.Version, nil
}

func (api *sabAPI) queue(ctx context.Context, nzbPath, archivePassword string, options queueOptions) (string, error) {
	file, err := os.Open(nzbPath)
	if err != nil {
		return "", fmt.Errorf("open NZB: %w", err)
	}
	defer file.Close()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("name", options.filename(nzbPath))
	if err != nil {
		return "", fmt.Errorf("create SABnzbd NZB request: %w", err)
	}
	if _, err := io.Copy(part, file); err != nil {
		return "", fmt.Errorf("copy NZB into SABnzbd request: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("close SABnzbd NZB request: %w", err)
	}
	params := url.Values{"mode": {"addfile"}, "output": {"json"}, "apikey": {apiKey}}
	if options.forceAccept {
		// SABnzbd's addfile API has no force-duplicate switch. An explicit NZB
		// name becomes the job name used by its duplicate matcher, so the twenty
		// intentionally identical uploads remain distinct queue entries.
		params.Set("nzbname", strings.TrimSuffix(options.filename(nzbPath), filepath.Ext(options.filename(nzbPath))))
	}
	if archivePassword != "" {
		params.Set("password", archivePassword)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.apiURL(params), &body)
	if err != nil {
		return "", fmt.Errorf("build SABnzbd queue request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	var response struct {
		Status bool     `json:"status"`
		NZOIDs []string `json:"nzo_ids"`
	}
	if err := decodeJSON(api.client, req, &response); err != nil {
		return "", fmt.Errorf("queue NZB in SABnzbd: %w", err)
	}
	if !response.Status || len(response.NZOIDs) != 1 || strings.TrimSpace(response.NZOIDs[0]) == "" {
		return "", fmt.Errorf("SABnzbd did not accept exactly one queued NZB")
	}
	return response.NZOIDs[0], nil
}

func (api *sabAPI) waitComplete(ctx context.Context, nzoID string, interval time.Duration) (time.Time, error) {
	return waitForTerminal(ctx, interval, func(ctx context.Context) (bool, error) {
		var response struct {
			History struct {
				Slots []map[string]any `json:"slots"`
			} `json:"history"`
		}
		if err := api.get(ctx, "history", url.Values{"limit": {"100"}}, &response); err != nil {
			return false, err
		}
		for _, slot := range response.History.Slots {
			if fieldString(slot, "nzo_id") != nzoID {
				continue
			}
			status := strings.ToLower(fieldString(slot, "status"))
			switch {
			case strings.Contains(status, "complete") || strings.Contains(status, "success"):
				return true, nil
			case strings.Contains(status, "fail"), strings.Contains(status, "delete"), strings.Contains(status, "abort"):
				return false, fmt.Errorf("SABnzbd history status %q", fieldString(slot, "status"))
			}
		}
		return false, nil
	})
}

func (api *sabAPI) observe(ctx context.Context, jobIDs []string) (map[string]jobObservation, error) {
	wanted := stringSet(jobIDs)
	observations := make(map[string]jobObservation, len(jobIDs))
	var queueResponse struct {
		Queue struct {
			Slots []map[string]any `json:"slots"`
		} `json:"queue"`
	}
	if err := api.get(ctx, "queue", nil, &queueResponse); err != nil {
		return nil, fmt.Errorf("observe SABnzbd queue: %w", err)
	}
	for _, slot := range queueResponse.Queue.Slots {
		id := fieldString(slot, "nzo_id")
		if wanted[id] {
			observations[id] = classifyLiveStatus(fieldString(slot, "status"))
		}
	}

	var historyResponse struct {
		History struct {
			Slots []map[string]any `json:"slots"`
		} `json:"history"`
	}
	historyLimit := len(jobIDs)
	if historyLimit < 100 {
		historyLimit = 100
	}
	if err := api.get(ctx, "history", url.Values{"limit": {strconv.Itoa(historyLimit)}}, &historyResponse); err != nil {
		return nil, fmt.Errorf("observe SABnzbd history: %w", err)
	}
	for _, slot := range historyResponse.History.Slots {
		id := fieldString(slot, "nzo_id")
		if !wanted[id] {
			continue
		}
		status := fieldString(slot, "status")
		normalized := strings.ToLower(status)
		switch {
		case strings.Contains(normalized, "complete"), strings.Contains(normalized, "success"):
			observations[id] = jobObservation{state: jobComplete, status: status}
		case strings.Contains(normalized, "fail"), strings.Contains(normalized, "delete"), strings.Contains(normalized, "abort"):
			observations[id] = jobObservation{state: jobFailed, status: status}
		default:
			observations[id] = jobObservation{state: jobActive, status: status}
		}
	}
	return observations, nil
}

func (api *sabAPI) get(ctx context.Context, mode string, extra url.Values, target any) error {
	params := url.Values{"mode": {mode}, "output": {"json"}, "apikey": {apiKey}}
	for key, values := range extra {
		params[key] = append([]string(nil), values...)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, api.apiURL(params), nil)
	if err != nil {
		return err
	}
	return decodeJSON(api.client, req, target)
}

func (api *sabAPI) apiURL(params url.Values) string {
	return strings.TrimRight(api.baseURL, "/") + "/api?" + params.Encode()
}

type nzbgetAPI struct {
	baseURL string
	client  *http.Client
}

type nzbgetPPParameter map[string]string

func nzbgetPPParameters(archivePassword string) []nzbgetPPParameter {
	parameters := make([]nzbgetPPParameter, 0, 1)
	if archivePassword != "" {
		parameters = append(parameters, nzbgetPPParameter{"*Unpack:Password": archivePassword})
	}
	return parameters
}

func (api *nzbgetAPI) waitReady(ctx context.Context) (string, error) {
	var raw json.RawMessage
	if err := api.rpc(ctx, "version", nil, &raw); err != nil {
		return "", err
	}
	var version string
	if err := json.Unmarshal(raw, &version); err != nil || strings.TrimSpace(version) == "" {
		return "", fmt.Errorf("NZBGet version response was invalid")
	}
	return version, nil
}

func (api *nzbgetAPI) queue(ctx context.Context, nzbPath, archivePassword string, options queueOptions) (string, error) {
	contents, err := os.ReadFile(nzbPath)
	if err != nil {
		return "", fmt.Errorf("read NZB: %w", err)
	}
	parameters := nzbgetPPParameters(archivePassword)
	dupeMode := "SCORE"
	if options.forceAccept {
		dupeMode = "FORCE"
	}
	params := nzbgetAppendParameters(options.filename(nzbPath), contents, parameters, dupeMode)
	var raw json.RawMessage
	if err := api.rpc(ctx, "append", params, &raw); err != nil {
		return "", fmt.Errorf("queue NZB in NZBGet: %w", err)
	}
	var id json.Number
	if err := json.Unmarshal(raw, &id); err != nil {
		return "", fmt.Errorf("NZBGet append did not return a numeric queue id")
	}
	parsed, err := strconv.ParseInt(id.String(), 10, 64)
	if err != nil || parsed < 1 {
		return "", fmt.Errorf("NZBGet append returned invalid queue id %q", id.String())
	}
	return id.String(), nil
}

func nzbgetAppendParameters(filename string, contents []byte, parameters []nzbgetPPParameter, dupeMode string) []any {
	return []any{
		filename,
		base64.StdEncoding.EncodeToString(contents),
		"", 0, false, false, "", 0, dupeMode, false, parameters,
	}
}

func (api *nzbgetAPI) waitComplete(ctx context.Context, nzbID string, interval time.Duration) (time.Time, error) {
	return waitForTerminal(ctx, interval, func(ctx context.Context) (bool, error) {
		var raw json.RawMessage
		if err := api.rpc(ctx, "history", nil, &raw); err != nil {
			return false, err
		}
		var records []map[string]any
		if err := json.Unmarshal(raw, &records); err != nil {
			return false, fmt.Errorf("decode NZBGet history: %w", err)
		}
		for _, record := range records {
			if fieldString(record, "NZBID") != nzbID {
				continue
			}
			status := strings.ToUpper(fieldString(record, "Status"))
			if nzbgetHistoryFailed(record, status) {
				return false, fmt.Errorf("NZBGet history status %q", fieldString(record, "Status"))
			}
			if nzbgetHistoryComplete(status) {
				return true, nil
			}
		}
		return false, nil
	})
}

func (api *nzbgetAPI) observe(ctx context.Context, jobIDs []string) (map[string]jobObservation, error) {
	wanted := stringSet(jobIDs)
	observations := make(map[string]jobObservation, len(jobIDs))
	var raw json.RawMessage
	if err := api.rpc(ctx, "listgroups", nil, &raw); err != nil {
		return nil, fmt.Errorf("observe NZBGet queue: %w", err)
	}
	var groups []map[string]any
	if err := json.Unmarshal(raw, &groups); err != nil {
		return nil, fmt.Errorf("decode NZBGet queue: %w", err)
	}
	for _, group := range groups {
		id := fieldString(group, "NZBID")
		if wanted[id] {
			observations[id] = classifyLiveStatus(fieldString(group, "Status"))
		}
	}

	if err := api.rpc(ctx, "history", nil, &raw); err != nil {
		return nil, fmt.Errorf("observe NZBGet history: %w", err)
	}
	var records []map[string]any
	if err := json.Unmarshal(raw, &records); err != nil {
		return nil, fmt.Errorf("decode NZBGet history: %w", err)
	}
	for _, record := range records {
		id := fieldString(record, "NZBID")
		if !wanted[id] {
			continue
		}
		status := strings.ToUpper(fieldString(record, "Status"))
		switch {
		case nzbgetHistoryFailed(record, status):
			observations[id] = jobObservation{state: jobFailed, status: fieldString(record, "Status")}
		case nzbgetHistoryComplete(status):
			observations[id] = jobObservation{state: jobComplete, status: fieldString(record, "Status")}
		default:
			observations[id] = jobObservation{state: jobActive, status: fieldString(record, "Status")}
		}
	}
	return observations, nil
}

func nzbgetHistoryComplete(status string) bool {
	return strings.HasPrefix(status, "SUCCESS") || status == "COMPLETED" || status == "COMPLETE"
}

func (api *nzbgetAPI) rpc(ctx context.Context, method string, params any, target *json.RawMessage) error {
	payload, err := json.Marshal(struct {
		Version string `json:"version"`
		Method  string `json:"method"`
		Params  any    `json:"params,omitempty"`
		ID      string `json:"id"`
	}{Version: "2.0", Method: method, Params: params, ID: "nntpbench"})
	if err != nil {
		return fmt.Errorf("encode NZBGet RPC request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(api.baseURL, "/")+"/jsonrpc", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build NZBGet RPC request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(controlUsername, apiKey)
	var response struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := decodeJSON(api.client, req, &response); err != nil {
		return err
	}
	if response.Error != nil {
		return fmt.Errorf("NZBGet RPC %s failed (%d): %s", method, response.Error.Code, response.Error.Message)
	}
	if len(response.Result) == 0 || string(response.Result) == "null" {
		return fmt.Errorf("NZBGet RPC %s returned no result", method)
	}
	*target = response.Result
	return nil
}

func nzbgetHistoryFailed(record map[string]any, status string) bool {
	if strings.Contains(status, "FAIL") || strings.Contains(status, "DELETE") || strings.Contains(status, "BAD") {
		return true
	}
	for _, field := range []string{"DeleteStatus", "ParStatus", "UnpackStatus", "MoveStatus", "ScriptStatus"} {
		value := strings.ToUpper(fieldString(record, field))
		if value != "" && value != "SUCCESS" && value != "NONE" {
			return true
		}
	}
	return false
}

type weaverAPI struct {
	baseURL string
	client  *http.Client
}

func (api *weaverAPI) waitReady(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(api.baseURL, "/")+"/", nil)
	if err != nil {
		return "", fmt.Errorf("build Weaver session request: %w", err)
	}
	response, err := api.client.Do(req)
	if err != nil {
		return "", err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	_ = response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("Weaver session endpoint returned HTTP %d", response.StatusCode)
	}
	var data struct {
		Typename string `json:"__typename"`
	}
	if err := api.graphQL(ctx, "query { __typename }", nil, &data); err != nil {
		return "", err
	}
	if data.Typename == "" {
		return "", fmt.Errorf("Weaver GraphQL readiness query returned no typename")
	}
	return "", nil
}

func (api *weaverAPI) queue(ctx context.Context, nzbPath, archivePassword string, options queueOptions) (string, error) {
	contents, err := os.ReadFile(nzbPath)
	if err != nil {
		return "", fmt.Errorf("read NZB: %w", err)
	}
	input := map[string]any{
		"nzbBase64": base64.StdEncoding.EncodeToString(contents),
		"filename":  options.filename(nzbPath),
	}
	if archivePassword != "" {
		input["password"] = archivePassword
	}
	if options.forceAccept {
		input["force"] = true
	}
	var data struct {
		SubmitNZB struct {
			Accepted bool `json:"accepted"`
			Item     struct {
				ID json.RawMessage `json:"id"`
			} `json:"item"`
		} `json:"submitNzb"`
	}
	query := "mutation Submit($input: SubmitNzbInput!) { submitNzb(input: $input) { accepted item { id } } }"
	if err := api.graphQL(ctx, query, map[string]any{"input": input}, &data); err != nil {
		return "", fmt.Errorf("queue NZB in Weaver: %w", err)
	}
	if !data.SubmitNZB.Accepted {
		return "", fmt.Errorf("Weaver did not accept queued NZB")
	}
	id, err := numericID(data.SubmitNZB.Item.ID)
	if err != nil {
		return "", fmt.Errorf("Weaver submission id: %w", err)
	}
	return id, nil
}

func (api *weaverAPI) waitComplete(ctx context.Context, jobID string, interval time.Duration) (time.Time, error) {
	return waitForTerminal(ctx, interval, func(ctx context.Context) (bool, error) {
		var data struct {
			Job *struct {
				Status string `json:"status"`
			} `json:"job"`
		}
		query := "query { job(id: " + jobID + ") { status } }"
		if err := api.graphQL(ctx, query, nil, &data); err != nil {
			return false, err
		}
		if data.Job == nil {
			return false, fmt.Errorf("Weaver job %s disappeared", jobID)
		}
		status := strings.ToUpper(data.Job.Status)
		switch {
		case strings.Contains(status, "COMPLETE") || strings.Contains(status, "SUCCESS"):
			return true, nil
		case strings.Contains(status, "FAIL"), strings.Contains(status, "CANCEL"), strings.Contains(status, "ERROR"):
			return false, fmt.Errorf("Weaver job status %q", data.Job.Status)
		default:
			return false, nil
		}
	})
}

func (api *weaverAPI) observe(ctx context.Context, jobIDs []string) (map[string]jobObservation, error) {
	var query strings.Builder
	query.WriteString("query {")
	for index, id := range jobIDs {
		if _, err := strconv.ParseUint(id, 10, 64); err != nil {
			return nil, fmt.Errorf("invalid Weaver job id %q", id)
		}
		fmt.Fprintf(&query, " j%d: job(id: %s) { status }", index, id)
	}
	query.WriteString(" }")
	var data map[string]*struct {
		Status string `json:"status"`
	}
	if err := api.graphQL(ctx, query.String(), nil, &data); err != nil {
		return nil, fmt.Errorf("observe Weaver queue: %w", err)
	}
	observations := make(map[string]jobObservation, len(jobIDs))
	for index, id := range jobIDs {
		job := data[fmt.Sprintf("j%d", index)]
		if job == nil {
			continue
		}
		status := strings.ToUpper(job.Status)
		switch {
		case strings.Contains(status, "COMPLETE"), strings.Contains(status, "SUCCESS"):
			observations[id] = jobObservation{state: jobComplete, status: job.Status}
		case strings.Contains(status, "FAIL"), strings.Contains(status, "CANCEL"), strings.Contains(status, "ERROR"):
			observations[id] = jobObservation{state: jobFailed, status: job.Status}
		default:
			observations[id] = classifyLiveStatus(job.Status)
		}
	}
	return observations, nil
}

func (api *weaverAPI) graphQL(ctx context.Context, query string, variables any, target any) error {
	payload, err := json.Marshal(struct {
		Query     string `json:"query"`
		Variables any    `json:"variables,omitempty"`
	}{Query: query, Variables: variables})
	if err != nil {
		return fmt.Errorf("encode Weaver GraphQL request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(api.baseURL, "/")+"/graphql", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build Weaver GraphQL request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	var response struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := decodeJSON(api.client, req, &response); err != nil {
		return err
	}
	if len(response.Errors) > 0 {
		return fmt.Errorf("Weaver GraphQL error: %s", response.Errors[0].Message)
	}
	if len(response.Data) == 0 || string(response.Data) == "null" {
		return fmt.Errorf("Weaver GraphQL returned no data")
	}
	if err := json.Unmarshal(response.Data, target); err != nil {
		return fmt.Errorf("decode Weaver GraphQL data: %w", err)
	}
	return nil
}

func decodeJSON(client *http.Client, request *http.Request, target any) error {
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
		return fmt.Errorf("HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<20))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode JSON response: %w", err)
	}
	return nil
}

func waitForTerminal(ctx context.Context, interval time.Duration, check func(context.Context) (bool, error)) (time.Time, error) {
	for {
		complete, err := check(ctx)
		if err != nil {
			return time.Time{}, err
		}
		if complete {
			return time.Now().UTC(), nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return time.Time{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func fieldString(fields map[string]any, name string) string {
	normalized := normalizeField(name)
	for key, value := range fields {
		if normalizeField(key) != normalized {
			continue
		}
		switch typed := value.(type) {
		case string:
			return typed
		case json.Number:
			return typed.String()
		case float64:
			return strconv.FormatInt(int64(typed), 10)
		}
	}
	return ""
}

func normalizeField(value string) string {
	value = strings.ToLower(value)
	value = strings.ReplaceAll(value, "_", "")
	return strings.ReplaceAll(value, "-", "")
}

func numericID(raw json.RawMessage) (string, error) {
	var value json.Number
	if err := json.Unmarshal(raw, &value); err == nil {
		if _, err := strconv.ParseUint(value.String(), 10, 64); err == nil {
			return value.String(), nil
		}
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if _, err := strconv.ParseUint(text, 10, 64); err == nil {
			return text, nil
		}
	}
	return "", fmt.Errorf("expected positive numeric id")
}
