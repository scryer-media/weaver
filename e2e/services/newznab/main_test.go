package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReleaseMatchesSearchStructuredFilters(t *testing.T) {
	rel := &Release{
		GUID:      "silver-horizon-guid",
		Title:     "Silver.Horizon.S01E01.720p.WEB-DL.AV1-TESTGRP",
		SizeBytes: 88 * 1024 * 1024,
		PubDate:   time.Unix(1704067200, 0).UTC(),
		Attributes: map[string]string{
			"category": "5000",
			"tvdbid":   "7000001",
			"rid":      "7000002",
			"tvmazeid": "7000003",
			"season":   "1",
			"ep":       "1",
		},
	}

	req := httptest.NewRequest("GET", "/api?t=tvsearch&q=silver+horizon&tvdbid=7000001&season=01&ep=1&cat=5000", nil)
	criteria := parseSearchCriteria(req)
	if !releaseMatchesSearch(rel, criteria) {
		t.Fatalf("expected release to match structured series search")
	}
}

func TestReleaseMatchesSearchNormalizesQueryTextAndIgnoresYearTokens(t *testing.T) {
	rel := &Release{
		GUID:      "silver-horizon-guid",
		Title:     "Silver.Horizon.S01E01.720p.WEB-DL.AV1-TESTGRP",
		SizeBytes: 88 * 1024 * 1024,
		PubDate:   time.Unix(1704067200, 0).UTC(),
		Attributes: map[string]string{
			"category": "5000",
			"tvdbid":   "7000001",
			"season":   "1",
			"ep":       "1",
		},
	}

	req := httptest.NewRequest(
		"GET",
		"/api?t=tvsearch&q=Silver+Horizon+(2018)+S01E01&tvdbid=7000001&season=01&ep=1&cat=5000",
		nil,
	)
	if !releaseMatchesSearch(rel, parseSearchCriteria(req)) {
		t.Fatalf("expected normalized query search to match fixture title")
	}
}

func TestReleaseMatchesSearchSupportsManagedTVIdentifiers(t *testing.T) {
	rel := &Release{
		GUID:      "silver-horizon-guid",
		Title:     "Silver.Horizon.S01E01.720p.WEB-DL.AV1-TESTGRP",
		SizeBytes: 88 * 1024 * 1024,
		PubDate:   time.Unix(1704067200, 0).UTC(),
		Attributes: map[string]string{
			"category": "5000",
			"tvdbid":   "7000001",
			"rid":      "7000002",
			"tvmazeid": "7000003",
			"season":   "1",
			"ep":       "1",
		},
	}

	for _, rawURL := range []string{
		"/api?t=tvsearch&q=silver+horizon&rid=7000002&season=1&ep=1&cat=5000",
		"/api?t=tvsearch&q=silver+horizon&tvmazeid=7000003&season=1&ep=1&cat=5000",
	} {
		req := httptest.NewRequest("GET", rawURL, nil)
		if !releaseMatchesSearch(rel, parseSearchCriteria(req)) {
			t.Fatalf("expected release to match managed TV identifier search %q", rawURL)
		}
	}
}

func TestReleaseMatchesSearchNormalizesIMDbAndAnimeCategory(t *testing.T) {
	rel := &Release{
		GUID:      "amber-trail-guid",
		Title:     "Amber.Trail.2012.720p.WEB-DL.AV1-TESTGRP",
		SizeBytes: 80 * 1024 * 1024,
		PubDate:   time.Unix(1704067200, 0).UTC(),
		Attributes: map[string]string{
			"category": "2000",
			"imdbid":   "7000004",
		},
	}

	req := httptest.NewRequest("GET", "/api?t=movie&imdbid=tt7000004", nil)
	if !releaseMatchesSearch(rel, parseSearchCriteria(req)) {
		t.Fatalf("expected movie release to match imdb search with tt prefix")
	}

	anime := &Release{
		GUID:      "anime-guid",
		Title:     "Moss.Compass.Diary.S01E01.720p.WEB-DL.AV1-TESTGRP",
		SizeBytes: 78 * 1024 * 1024,
		PubDate:   time.Unix(1704067200, 0).UTC(),
		Attributes: map[string]string{
			"category": "5070",
			"tvdbid":   "7000006",
			"season":   "1",
			"ep":       "1",
			"anidbid":  "7000007",
		},
	}
	animeReq := httptest.NewRequest("GET", "/api?t=tvsearch&tvdbid=7000006&season=1&ep=01&cat=5070", nil)
	if !releaseMatchesSearch(anime, parseSearchCriteria(animeReq)) {
		t.Fatalf("expected anime release to match category-aware series search")
	}
}

func TestReleaseMatchesSearchHonorsExplicitMovieCategoryFilters(t *testing.T) {
	rel := &Release{
		GUID:      "quartz-harbor-case1-guid",
		Title:     "Quartz.Harbor.Chronicles.Case.1.Ash.and.Ember.2019.720p.WEB-DL.AV1.AAC2.0-NTb",
		SizeBytes: 900 * 1024 * 1024,
		PubDate:   time.Unix(1704067200, 0).UTC(),
		Attributes: map[string]string{
			"category": "5070",
			"imdbid":   "7000005",
		},
	}

	req := httptest.NewRequest(
		"GET",
		"/api?t=movie&q=Quartz+Harbor+Chronicles%3A+Case.1+Ash+and+Ember+2019&imdbid=tt7000005&cat=2000,5070",
		nil,
	)
	if !releaseMatchesSearch(rel, parseSearchCriteria(req)) {
		t.Fatalf("expected explicit category filters to allow movie-shaped search for anime movie release")
	}

	req = httptest.NewRequest(
		"GET",
		"/api?t=movie&q=Quartz+Harbor+Chronicles%3A+Case.1+Ash+and+Ember+2019&imdbid=tt7000005",
		nil,
	)
	if releaseMatchesSearch(rel, parseSearchCriteria(req)) {
		t.Fatalf("expected movie endpoint defaults to reject anime category without explicit category filter")
	}
}

func TestReleaseMatchesSearchSupportsPunctuationStrippedTitleQueries(t *testing.T) {
	rel := &Release{
		GUID:      "quartz-harbor-case3-guid",
		Title:     "Quartz.Harbor.Chronicles.Case.3.Beyond.the.Tideline.2019.720p.WEB-DL.AV1.AAC2.0-NTb",
		SizeBytes: 900 * 1024 * 1024,
		PubDate:   time.Unix(1704067200, 0).UTC(),
		Attributes: map[string]string{
			"category": "5070",
			"imdbid":   "7000008",
			"tvdbid":   "7000009",
		},
	}

	req := httptest.NewRequest(
		"GET",
		"/api?t=movie&q=Quartz+Harbor+Chronicles+Case3+Beyond+the+Tideline&cat=6050,2000,5070",
		nil,
	)
	if !releaseMatchesSearch(rel, parseSearchCriteria(req)) {
		t.Fatalf("expected punctuation-stripped Case3 query to match Case.3 release title")
	}
}

func TestQueryMatchesTitleSupportsCleanedSceneTitles(t *testing.T) {
	for _, tt := range []struct {
		query string
		title string
	}{
		{
			query: "Harbor PD",
			title: "Harbor.P.D.S01E01.720p.WEB-DL.AV1-TESTGRP",
		},
		{
			query: "Kestrel and Vale",
			title: "Kestrel.&.Vale.S01E01.720p.WEB-DL.AV1-TESTGRP",
		},
		{
			query: "Meridian Nine 0",
			title: "Meridian.Nine-0.S01E01.720p.WEB-DL.AV1-TESTGRP",
		},
		{
			query: "Wren Ashfords Off The Rails",
			title: "Wren.Ashford's.Off.The.Rails.S01E01.720p.WEB-DL.AV1-TESTGRP",
		},
	} {
		if !queryMatchesTitle(tt.query, tt.title) {
			t.Fatalf("expected query %q to match title %q", tt.query, tt.title)
		}
	}
}

func TestReleaseMatchesSearchTreatsParentCategoriesAsIncludingChildren(t *testing.T) {
	rel := &Release{
		GUID:      "amber-trail-hd-guid",
		Title:     "Amber.Trail.2012.720p.WEB-DL.AV1-TESTGRP",
		SizeBytes: 80 * 1024 * 1024,
		PubDate:   time.Unix(1704067200, 0).UTC(),
		Attributes: map[string]string{
			"category": "2040",
			"imdbid":   "7000004",
		},
	}

	req := httptest.NewRequest("GET", "/api?t=movie&imdbid=tt7000004&cat=2000", nil)
	if !releaseMatchesSearch(rel, parseSearchCriteria(req)) {
		t.Fatalf("expected movie parent category to match child movie category")
	}

	req = httptest.NewRequest("GET", "/api?t=movie&imdbid=tt7000004", nil)
	if !releaseMatchesSearch(rel, parseSearchCriteria(req)) {
		t.Fatalf("expected movie endpoint defaults to include child movie category")
	}
}

func TestReleaseMatchesSearchRejectsMismatchedFilters(t *testing.T) {
	rel := &Release{
		GUID:      "silver-horizon-guid",
		Title:     "Silver.Horizon.S01E01.720p.WEB-DL.AV1-TESTGRP",
		SizeBytes: 88 * 1024 * 1024,
		PubDate:   time.Unix(1704067200, 0).UTC(),
		Attributes: map[string]string{
			"category": "5000",
			"tvdbid":   "7000001",
			"season":   "1",
			"ep":       "1",
		},
	}

	req := httptest.NewRequest("GET", "/api?t=movie&imdbid=tt9999999", nil)
	if releaseMatchesSearch(rel, parseSearchCriteria(req)) {
		t.Fatalf("expected movie endpoint mismatch to reject series release")
	}

	req = httptest.NewRequest("GET", "/api?t=tvsearch&tvdbid=7000001&season=1&ep=2", nil)
	if releaseMatchesSearch(rel, parseSearchCriteria(req)) {
		t.Fatalf("expected episode mismatch to reject release")
	}
}

func TestServeSearchJSONPreservesRawDownloadURL(t *testing.T) {
	mu.Lock()
	previousReleases := releases
	releases = map[string]*Release{
		"silver-horizon-guid": {
			GUID:      "silver-horizon-guid",
			Title:     "Silver.Horizon.S01E01.720p.DSNP.WEB-DL.AV1.AAC2.0-NTb",
			SizeBytes: 77_514_027,
			PubDate:   time.Unix(1704067200, 0).UTC(),
			Attributes: map[string]string{
				"category": "5000",
				"tvdbid":   "7000001",
				"season":   "1",
				"ep":       "1",
			},
		},
	}
	mu.Unlock()

	t.Cleanup(func() {
		mu.Lock()
		releases = previousReleases
		mu.Unlock()
	})

	req := httptest.NewRequest(
		"GET",
		"http://newznab:8088/api?t=tvsearch&o=json&apikey=test-e2e-key&tvdbid=7000001&season=1&ep=1",
		nil,
	)
	rec := httptest.NewRecorder()

	serveSearch(rec, req)

	if rec.Code != 200 {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp jsonResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("expected JSON response, got error: %v", err)
	}
	if len(resp.Channel.Items) != 1 {
		t.Fatalf("expected one item, got %d", len(resp.Channel.Items))
	}

	downloadURL := resp.Channel.Items[0].Enclosure.Attributes.URL
	expected := "http://newznab:8088/api?t=get&id=silver-horizon-guid&apikey=test-e2e-key"
	if downloadURL != expected {
		t.Fatalf("expected download URL %q, got %q", expected, downloadURL)
	}
}

func TestServeCapsAdvertisesSupportedSearchParams(t *testing.T) {
	rec := httptest.NewRecorder()

	serveCaps(rec)

	if rec.Code != 200 {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	for _, expected := range []string{
		`<search available="yes" supportedParams="q" />`,
		`<tv-search available="yes" supportedParams="q,tvdbid,rid,tvmazeid,season,ep" />`,
		`<movie-search available="yes" supportedParams="q,imdbid" />`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("expected caps payload to include %q, got %s", expected, body)
		}
	}
}

func TestServeSearchRecordsStructuredSearchRequests(t *testing.T) {
	mu.Lock()
	previousSearchRecords := append([]SearchRequestRecord(nil), searchRecords...)
	searchRecords = nil
	mu.Unlock()

	t.Cleanup(func() {
		mu.Lock()
		searchRecords = previousSearchRecords
		mu.Unlock()
	})

	req := httptest.NewRequest(
		"GET",
		"http://newznab:8088/api?t=tvsearch&apikey=test-e2e-key&tvdbid=7000001&rid=7000002&tvmazeid=7000003&season=1&ep=1&cat=5000",
		nil,
	)
	rec := httptest.NewRecorder()

	serveSearch(rec, req)

	searchesRec := httptest.NewRecorder()
	searchesReq := httptest.NewRequest("GET", "/admin/searches", nil)
	handleAdminSearches(searchesRec, searchesReq)

	if searchesRec.Code != 200 {
		t.Fatalf("expected search admin status 200, got %d", searchesRec.Code)
	}

	var got []SearchRequestRecord
	if err := json.Unmarshal(searchesRec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode search admin response: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one recorded search, got %d", len(got))
	}
	if got[0].Endpoint != "tvsearch" {
		t.Fatalf("expected tvsearch endpoint, got %q", got[0].Endpoint)
	}
	if got[0].Identifiers["tvdbid"] != "7000001" {
		t.Fatalf("expected tvdbid 7000001, got %#v", got[0].Identifiers)
	}
	if got[0].Identifiers["rid"] != "7000002" {
		t.Fatalf("expected rid 7000002, got %#v", got[0].Identifiers)
	}
	if got[0].Identifiers["tvmazeid"] != "7000003" {
		t.Fatalf("expected tvmazeid 7000003, got %#v", got[0].Identifiers)
	}
	if len(got[0].Categories) != 1 || got[0].Categories[0] != "5000" {
		t.Fatalf("expected categories [5000], got %#v", got[0].Categories)
	}
}

func TestChallengeAPIRequiresBrowserCookieAndTracksClearedRequests(t *testing.T) {
	mu.Lock()
	previousStats := challengeStats
	challengeStats = ChallengeStats{}
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		challengeStats = previousStats
		mu.Unlock()
	})

	capsReq := httptest.NewRequest("GET", "http://newznab:8088/challenge/api?t=caps", nil)
	capsRec := httptest.NewRecorder()
	handleChallengeAPI(capsRec, capsReq, "test-e2e-key")
	if capsRec.Code != http.StatusOK || !strings.Contains(capsRec.Body.String(), "<caps>") {
		t.Fatalf("expected capability discovery to bypass the content challenge, got %d: %s", capsRec.Code, capsRec.Body.String())
	}
	connectionReq := httptest.NewRequest("GET", "http://newznab:8088/challenge/api?t=search&q=e2e%20connection%20test&apikey=test-e2e-key", nil)
	connectionRec := httptest.NewRecorder()
	handleChallengeAPI(connectionRec, connectionReq, "test-e2e-key")
	if connectionRec.Code != http.StatusOK || !strings.Contains(connectionRec.Body.String(), "<rss") {
		t.Fatalf("expected connection test to bypass the content challenge, got %d: %s", connectionRec.Code, connectionRec.Body.String())
	}

	challengeReq := httptest.NewRequest("GET", "http://newznab:8088/challenge/api?t=movie&q=amber+trail&apikey=test-e2e-key", nil)
	challengeRec := httptest.NewRecorder()
	handleChallengeAPI(challengeRec, challengeReq, "test-e2e-key")
	if challengeRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected challenge status 503, got %d", challengeRec.Code)
	}
	if !strings.Contains(challengeRec.Body.String(), "cf-chl-e2e") {
		t.Fatalf("expected deterministic challenge marker, got %s", challengeRec.Body.String())
	}

	clearedReq := httptest.NewRequest("GET", "http://newznab:8088/challenge/api?t=movie&q=amber+trail&apikey=test-e2e-key", nil)
	clearedReq.AddCookie(&http.Cookie{Name: "e2e_clearance", Value: "solved"})
	clearedReq.Header.Set("User-Agent", "Mozilla/5.0 e2e")
	clearedRec := httptest.NewRecorder()
	handleChallengeAPI(clearedRec, clearedReq, "test-e2e-key")
	if clearedRec.Code != http.StatusOK {
		t.Fatalf("expected cleared status 200, got %d", clearedRec.Code)
	}
	if !strings.Contains(clearedRec.Body.String(), "<rss") {
		t.Fatalf("expected search response after clearance, got %s", clearedRec.Body.String())
	}

	mu.RLock()
	got := challengeStats
	mu.RUnlock()
	if got.ChallengesServed != 1 || got.ClearedRequests != 1 || got.ClearedSearchRequests != 1 || got.ClearedDirectRequests != 1 {
		t.Fatalf("unexpected challenge stats: %#v", got)
	}
	if got.LastClearedUserAgent != "Mozilla/5.0 e2e" {
		t.Fatalf("expected cleared user agent to be recorded, got %q", got.LastClearedUserAgent)
	}
}

func TestChallengeSearchKeepsDownloadURLUnderChallengePath(t *testing.T) {
	mu.Lock()
	previousReleases := releases
	releases = map[string]*Release{
		"amber-trail-guid": {
			GUID:      "amber-trail-guid",
			Title:     "Amber.Trail.2012.720p.WEB-DL.AV1.AAC2.0-NTb",
			SizeBytes: 1024,
			PubDate:   time.Unix(1704067200, 0).UTC(),
			Attributes: map[string]string{
				"category": "2000",
			},
		},
	}
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		releases = previousReleases
		mu.Unlock()
	})

	req := httptest.NewRequest(
		"GET",
		"http://newznab:8088/challenge/api?t=movie&o=json&apikey=test-e2e-key&q=Amber+Trail",
		nil,
	)
	req.AddCookie(&http.Cookie{Name: "e2e_clearance", Value: "solved"})
	rec := httptest.NewRecorder()
	handleChallengeAPI(rec, req, "test-e2e-key")

	var resp jsonResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode challenge search response: %v", err)
	}
	if len(resp.Channel.Items) != 1 {
		t.Fatalf("expected one item, got %d", len(resp.Channel.Items))
	}
	want := "http://newznab:8088/challenge/api?t=get&id=amber-trail-guid&apikey=test-e2e-key"
	if got := resp.Channel.Items[0].Enclosure.Attributes.URL; got != want {
		t.Fatalf("expected challenge download URL %q, got %q", want, got)
	}
}
