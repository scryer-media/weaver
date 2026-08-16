// Lightweight Newznab-compatible indexer for e2e testing.
// Implements the subset of the Newznab API a downloader's indexer client
// consumes: caps, search, tvsearch, movie, and NZB download (t=get).
//
// Releases are registered via a REST admin API (POST /admin/releases).
package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Release struct {
	GUID       string            `json:"guid"`
	Title      string            `json:"title"`
	NzbXML     []byte            `json:"nzb_xml"`
	SizeBytes  int64             `json:"size_bytes"`
	PubDate    time.Time         `json:"pub_date"`
	Attributes map[string]string `json:"attributes"`
}

type SearchRequestRecord struct {
	Timestamp   time.Time         `json:"timestamp"`
	Endpoint    string            `json:"endpoint"`
	Query       string            `json:"query"`
	Identifiers map[string]string `json:"identifiers"`
	Categories  []string          `json:"categories"`
	RawURL      string            `json:"raw_url"`
}

type ChallengeStats struct {
	ChallengesServed        int    `json:"challengesServed"`
	ClearedRequests         int    `json:"clearedRequests"`
	ClearedSearchRequests   int    `json:"clearedSearchRequests"`
	ClearedDownloadRequests int    `json:"clearedDownloadRequests"`
	ClearedDirectRequests   int    `json:"clearedDirectRequests"`
	LastClearedUserAgent    string `json:"lastClearedUserAgent"`
}

var (
	mu             sync.RWMutex
	releases       = map[string]*Release{}
	searchRecords  []SearchRequestRecord
	challengeStats ChallengeStats
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = "0.0.0.0:8088"
	}
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		apiKey = "test-e2e-key"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		handleAPI(w, r, apiKey)
	})
	mux.HandleFunc("/challenge/api", func(w http.ResponseWriter, r *http.Request) {
		handleChallengeAPI(w, r, apiKey)
	})
	mux.HandleFunc("/admin/releases", handleAdminReleases)
	mux.HandleFunc("/admin/searches", handleAdminSearches)
	mux.HandleFunc("/admin/challenge-stats", handleAdminChallengeStats)
	mux.HandleFunc("/admin/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"ok"}`)
	})

	log.Printf("newznab-api listening on %s (apikey=%s)", addr, apiKey)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func handleChallengeAPI(w http.ResponseWriter, r *http.Request, apiKey string) {
	query := r.URL.Query()
	if query.Get("t") == "caps" || query.Get("q") == "e2e connection test" {
		handleAPI(w, r, apiKey)
		return
	}

	cookie, err := r.Cookie("e2e_clearance")
	if err != nil || cookie.Value != "solved" {
		mu.Lock()
		challengeStats.ChallengesServed++
		mu.Unlock()
		serveBrowserChallenge(w)
		return
	}

	mu.Lock()
	challengeStats.ClearedRequests++
	challengeStats.LastClearedUserAgent = r.UserAgent()
	if r.Header.Get("Sec-Fetch-Mode") == "" {
		challengeStats.ClearedDirectRequests++
	}
	switch r.URL.Query().Get("t") {
	case "search", "tvsearch", "movie":
		challengeStats.ClearedSearchRequests++
	case "get":
		challengeStats.ClearedDownloadRequests++
	}
	mu.Unlock()

	handleAPI(w, r, apiKey)
}

func serveBrowserChallenge(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprint(w, `<!doctype html>
<html>
  <head><title>Just a moment</title></head>
  <body>
    <div id="cf-chl-e2e">Preparing the indexer session.</div>
    <script>
      document.cookie = "e2e_clearance=solved; Path=/; SameSite=Lax";
      window.location.reload();
    </script>
  </body>
</html>`)
}

func handleAdminChallengeStats(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		mu.RLock()
		stats := challengeStats
		mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case http.MethodDelete:
		mu.Lock()
		challengeStats = ChallengeStats{}
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleAPI(w http.ResponseWriter, r *http.Request, apiKey string) {
	q := r.URL.Query()
	t := q.Get("t")

	// Validate API key for non-caps requests
	if t != "caps" {
		if k := q.Get("apikey"); k != apiKey {
			http.Error(w, "invalid api key", http.StatusUnauthorized)
			return
		}
	}

	switch t {
	case "caps":
		serveCaps(w)
	case "search", "tvsearch", "movie":
		serveSearch(w, r)
	case "get":
		serveNZB(w, r)
	default:
		http.Error(w, "unsupported t parameter", http.StatusBadRequest)
	}
}

// --- Caps ---

func serveCaps(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server title="weaver-e2e-indexer" />
  <limits max="100" default="100" />
  <searching>
    <search available="yes" supportedParams="q" />
    <tv-search available="yes" supportedParams="q,tvdbid,rid,tvmazeid,season,ep" />
    <movie-search available="yes" supportedParams="q,imdbid" />
  </searching>
  <categories>
    <category id="2000" name="Movies" />
    <category id="2040" name="Movies HD" />
    <category id="5000" name="TV" />
    <category id="5070" name="Anime" />
  </categories>
</caps>`)
}

// --- Search ---

type rssAttr struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

type jsonResponse struct {
	Channel jsonChannel `json:"channel"`
}

type jsonChannel struct {
	Title string     `json:"title"`
	Items []jsonItem `json:"item"`
}

type jsonItem struct {
	Title     string         `json:"title"`
	Link      string         `json:"link"`
	PubDate   string         `json:"pubDate"`
	Enclosure jsonEnclosure  `json:"enclosure"`
	Attrs     []jsonItemAttr `json:"attr"`
}

type jsonEnclosure struct {
	Attributes jsonEnclosureAttributes `json:"@attributes"`
}

type jsonEnclosureAttributes struct {
	URL    string `json:"url"`
	Length string `json:"length"`
	Type   string `json:"type"`
}

type jsonItemAttr struct {
	Attributes jsonItemAttrAttributes `json:"@attributes"`
}

type jsonItemAttrAttributes struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type searchCriteria struct {
	endpoint   string
	query      string
	imdbID     string
	tmdbID     string
	tvdbID     string
	rID        string
	traktID    string
	tvMazeID   string
	anidbID    string
	season     string
	episode    string
	categories map[string]struct{}
}

func serveSearch(w http.ResponseWriter, r *http.Request) {
	criteria := parseSearchCriteria(r)
	basePath := strings.TrimSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/api")
	baseURL := fmt.Sprintf("http://%s%s", r.Host, basePath)
	apiKey := r.URL.Query().Get("apikey")
	responseFormat := normalizeValue(r.URL.Query().Get("o"))
	recordSearchRequest(r, criteria)

	mu.RLock()
	var items []searchResponseItem
	for _, rel := range releases {
		if !releaseMatchesSearch(rel, criteria) {
			continue
		}

		downloadURL := fmt.Sprintf("%s/api?t=get&id=%s&apikey=%s", baseURL, rel.GUID, apiKey)

		attrs := []rssAttr{
			{Name: "guid", Value: rel.GUID},
			{Name: "size", Value: fmt.Sprintf("%d", rel.SizeBytes)},
		}
		for k, v := range rel.Attributes {
			attrs = append(attrs, rssAttr{Name: k, Value: v})
		}
		if _, ok := rel.Attributes["password"]; !ok {
			attrs = append(attrs, rssAttr{Name: "password", Value: "0"})
		}
		sort.Slice(attrs, func(i, j int) bool {
			return attrs[i].Name < attrs[j].Name
		})
		items = append(items, searchResponseItem{
			Title:       rel.Title,
			Link:        downloadURL,
			PubDate:     rel.PubDate.Format(time.RFC1123Z),
			DownloadURL: downloadURL,
			SizeBytes:   rel.SizeBytes,
			Attrs:       attrs,
		})
	}
	mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].Title < items[j].Title
	})

	if responseFormat == "json" {
		serveSearchJSON(w, items)
		return
	}

	serveSearchRSS(w, items)
}

func recordSearchRequest(r *http.Request, criteria searchCriteria) {
	identifiers := map[string]string{}
	if criteria.imdbID != "" {
		identifiers["imdbid"] = criteria.imdbID
	}
	if criteria.tmdbID != "" {
		identifiers["tmdbid"] = criteria.tmdbID
	}
	if criteria.tvdbID != "" {
		identifiers["tvdbid"] = criteria.tvdbID
	}
	if criteria.rID != "" {
		identifiers["rid"] = criteria.rID
	}
	if criteria.traktID != "" {
		identifiers["traktid"] = criteria.traktID
	}
	if criteria.tvMazeID != "" {
		identifiers["tvmazeid"] = criteria.tvMazeID
	}
	if criteria.anidbID != "" {
		identifiers["anidbid"] = criteria.anidbID
	}

	categories := make([]string, 0, len(criteria.categories))
	for category := range criteria.categories {
		categories = append(categories, category)
	}
	sort.Strings(categories)

	record := SearchRequestRecord{
		Timestamp:   time.Now().UTC(),
		Endpoint:    criteria.endpoint,
		Query:       criteria.query,
		Identifiers: identifiers,
		Categories:  categories,
		RawURL:      r.URL.String(),
	}

	mu.Lock()
	searchRecords = append(searchRecords, record)
	mu.Unlock()
}

func handleAdminSearches(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		mu.RLock()
		records := append([]SearchRequestRecord(nil), searchRecords...)
		mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(records); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case http.MethodDelete:
		mu.Lock()
		searchRecords = nil
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type searchResponseItem struct {
	Title       string
	Link        string
	PubDate     string
	DownloadURL string
	SizeBytes   int64
	Attrs       []rssAttr
}

func serveSearchRSS(w http.ResponseWriter, items []searchResponseItem) {
	var builder strings.Builder
	builder.WriteString(xml.Header)
	builder.WriteString(`<rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">`)
	builder.WriteString("\n  <channel>\n")
	writeXMLTextElement(&builder, "title", "weaver-e2e-indexer", 4)
	for _, item := range items {
		builder.WriteString("    <item>\n")
		writeXMLTextElement(&builder, "title", item.Title, 6)
		writeXMLTextElement(&builder, "link", item.Link, 6)
		writeXMLTextElement(&builder, "pubDate", item.PubDate, 6)
		fmt.Fprintf(
			&builder,
			"      <enclosure url=\"%s\" length=\"%d\" type=\"application/x-nzb\" />\n",
			xmlEscaped(item.DownloadURL),
			item.SizeBytes,
		)
		for _, attr := range item.Attrs {
			fmt.Fprintf(
				&builder,
				"      <newznab:attr name=\"%s\" value=\"%s\" />\n",
				xmlEscaped(attr.Name),
				xmlEscaped(attr.Value),
			)
		}
		builder.WriteString("    </item>\n")
	}
	builder.WriteString("  </channel>\n</rss>")

	w.Header().Set("Content-Type", "application/rss+xml")
	fmt.Fprint(w, builder.String())
}

func writeXMLTextElement(builder *strings.Builder, name, value string, indent int) {
	builder.WriteString(strings.Repeat(" ", indent))
	fmt.Fprintf(builder, "<%s>", name)
	_ = xml.EscapeText(builder, []byte(value))
	fmt.Fprintf(builder, "</%s>\n", name)
}

func xmlEscaped(value string) string {
	var builder strings.Builder
	_ = xml.EscapeText(&builder, []byte(value))
	return builder.String()
}

func serveSearchJSON(w http.ResponseWriter, items []searchResponseItem) {
	resp := jsonResponse{
		Channel: jsonChannel{
			Title: "weaver-e2e-indexer",
			Items: make([]jsonItem, 0, len(items)),
		},
	}
	for _, item := range items {
		jsonAttrs := make([]jsonItemAttr, 0, len(item.Attrs))
		for _, attr := range item.Attrs {
			jsonAttrs = append(jsonAttrs, jsonItemAttr{
				Attributes: jsonItemAttrAttributes{
					Name:  attr.Name,
					Value: attr.Value,
				},
			})
		}
		resp.Channel.Items = append(resp.Channel.Items, jsonItem{
			Title:   item.Title,
			Link:    item.Link,
			PubDate: item.PubDate,
			Enclosure: jsonEnclosure{
				Attributes: jsonEnclosureAttributes{
					URL:    item.DownloadURL,
					Length: fmt.Sprintf("%d", item.SizeBytes),
					Type:   "application/x-nzb",
				},
			},
			Attrs: jsonAttrs,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func parseSearchCriteria(r *http.Request) searchCriteria {
	query := r.URL.Query()
	return searchCriteria{
		endpoint:   normalizeValue(query.Get("t")),
		query:      normalizeSearchText(query.Get("q")),
		imdbID:     normalizeIMDbID(query.Get("imdbid")),
		tmdbID:     normalizeValue(query.Get("tmdbid")),
		tvdbID:     normalizeValue(query.Get("tvdbid")),
		rID:        normalizeValue(query.Get("rid")),
		traktID:    normalizeValue(query.Get("traktid")),
		tvMazeID:   normalizeValue(query.Get("tvmazeid")),
		anidbID:    normalizeValue(query.Get("anidbid")),
		season:     normalizeNumericToken(query.Get("season")),
		episode:    normalizeNumericToken(query.Get("ep")),
		categories: parseCategoryFilter(query.Get("cat")),
	}
}

func releaseMatchesSearch(rel *Release, criteria searchCriteria) bool {
	if rel == nil {
		return false
	}

	if criteria.query != "" && !queryMatchesTitle(criteria.query, rel.Title) {
		return false
	}

	attrs := rel.Attributes
	if attrs == nil {
		attrs = map[string]string{}
	}

	category := normalizeValue(attrs["category"])
	if len(criteria.categories) > 0 {
		if !categoryMatchesFilter(category, criteria.categories) {
			return false
		}
	} else if !matchesEndpointDefaultCategory(criteria.endpoint, category) {
		return false
	}

	if criteria.imdbID != "" && normalizeIMDbID(attrs["imdbid"]) != criteria.imdbID {
		return false
	}
	if criteria.tmdbID != "" && normalizeValue(attrs["tmdbid"]) != criteria.tmdbID {
		return false
	}
	if criteria.tvdbID != "" && normalizeValue(attrs["tvdbid"]) != criteria.tvdbID {
		return false
	}
	if criteria.rID != "" && normalizeValue(attrs["rid"]) != criteria.rID {
		return false
	}
	if criteria.traktID != "" && normalizeValue(attrs["traktid"]) != criteria.traktID {
		return false
	}
	if criteria.tvMazeID != "" && normalizeValue(attrs["tvmazeid"]) != criteria.tvMazeID {
		return false
	}
	if criteria.anidbID != "" && normalizeValue(attrs["anidbid"]) != criteria.anidbID {
		return false
	}
	if criteria.season != "" && normalizeNumericToken(attrs["season"]) != criteria.season {
		return false
	}
	if criteria.episode != "" && normalizeNumericToken(attrs["ep"]) != criteria.episode {
		return false
	}

	return true
}

func matchesEndpointDefaultCategory(endpoint, category string) bool {
	if category == "" {
		return true
	}
	switch endpoint {
	case "movie":
		return categoryMatchesAny(category, "2000")
	case "tvsearch":
		return categoryMatchesAny(category, "5000", "5070")
	default:
		return true
	}
}

func categoryMatchesFilter(category string, filters map[string]struct{}) bool {
	if _, ok := filters[category]; ok {
		return true
	}
	for filter := range filters {
		if categoryIsChildOf(category, filter) {
			return true
		}
	}
	return false
}

func categoryMatchesAny(category string, filters ...string) bool {
	for _, filter := range filters {
		if category == filter || categoryIsChildOf(category, filter) {
			return true
		}
	}
	return false
}

func categoryIsChildOf(category, parent string) bool {
	return len(category) == 4 &&
		len(parent) == 4 &&
		parent[1:] == "000" &&
		category != parent &&
		category[:1] == parent[:1]
}

func parseCategoryFilter(raw string) map[string]struct{} {
	parts := strings.Split(raw, ",")
	set := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		if normalized := normalizeValue(part); normalized != "" {
			set[normalized] = struct{}{}
		}
	}
	return set
}

func normalizeIMDbID(raw string) string {
	normalized := normalizeValue(raw)
	normalized = strings.TrimPrefix(normalized, "tt")
	return normalized
}

func queryMatchesTitle(query, title string) bool {
	if normalizedTitleMatchesQuery(query, title, normalizeSearchText) {
		return true
	}
	return normalizedTitleMatchesQuery(query, title, normalizeSphinxSearchText)
}

func normalizedTitleMatchesQuery(query, title string, normalize func(string) string) bool {
	normalizedQuery := normalize(query)
	if normalizedQuery == "" {
		return true
	}
	normalizedTitle := normalize(title)
	if strings.Contains(normalizedTitle, normalizedQuery) {
		return true
	}

	requiredTokenCount := 0
	for _, token := range strings.Fields(normalizedQuery) {
		if isOptionalSearchToken(token) {
			continue
		}
		requiredTokenCount++
		if !strings.Contains(normalizedTitle, token) {
			return false
		}
	}

	if requiredTokenCount == 0 {
		return strings.Contains(normalizedTitle, normalizedQuery)
	}
	return true
}

func isOptionalSearchToken(token string) bool {
	if len(token) != 4 {
		return false
	}
	for _, ch := range token {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return token >= "1900" && token <= "2099"
}

func normalizeNumericToken(raw string) string {
	normalized := strings.TrimSpace(raw)
	normalized = strings.TrimLeft(normalized, "0")
	if normalized == "" {
		return strings.TrimSpace(raw)
	}
	return normalized
}

func normalizeValue(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func normalizeSearchText(raw string) string {
	normalized := normalizeValue(raw)
	replacer := strings.NewReplacer(
		".", " ",
		"_", " ",
		"-", " ",
		":", " ",
		"/", " ",
		"(", " ",
		")", " ",
		"[", " ",
		"]", " ",
		"{", " ",
		"}", " ",
	)
	normalized = replacer.Replace(normalized)
	return strings.Join(strings.Fields(normalized), " ")
}

func normalizeSphinxSearchText(raw string) string {
	normalized := normalizeValue(raw)
	normalized = strings.ReplaceAll(normalized, "&", " and ")

	var builder strings.Builder
	for _, ch := range normalized {
		switch ch {
		case '\'', '.', '`', '\u00B4', '\u2018', '\u2019':
			continue
		default:
			if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') {
				builder.WriteRune(ch)
			} else {
				builder.WriteByte(' ')
			}
		}
	}

	return strings.Join(strings.Fields(builder.String()), " ")
}

// --- NZB Download ---

func serveNZB(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	mu.RLock()
	rel, ok := releases[id]
	mu.RUnlock()
	if !ok {
		http.Error(w, "release not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/x-nzb")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.nzb"`, rel.GUID))
	_, _ = w.Write(rel.NzbXML)
}

// --- Admin API ---

func handleAdminReleases(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var rel Release
		if err := json.NewDecoder(r.Body).Decode(&rel); err != nil {
			http.Error(w, fmt.Sprintf("invalid json: %v", err), http.StatusBadRequest)
			return
		}
		if rel.GUID == "" || rel.Title == "" {
			http.Error(w, "guid and title are required", http.StatusBadRequest)
			return
		}
		if rel.PubDate.IsZero() {
			rel.PubDate = time.Now()
		}
		mu.Lock()
		releases[rel.GUID] = &rel
		mu.Unlock()
		log.Printf("registered release: guid=%s title=%s size=%d", rel.GUID, rel.Title, rel.SizeBytes)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"guid":%q}`, rel.GUID)

	case http.MethodGet:
		mu.RLock()
		list := make([]map[string]interface{}, 0, len(releases))
		for _, rel := range releases {
			list = append(list, map[string]interface{}{
				"guid":       rel.GUID,
				"title":      rel.Title,
				"size_bytes": rel.SizeBytes,
			})
		}
		mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(list)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
