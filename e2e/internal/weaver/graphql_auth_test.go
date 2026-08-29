package weaver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestGraphQLClientUsesBrowserSessionCookie(t *testing.T) {
	var uiLoads int
	var graphqlRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			uiLoads++
			http.SetCookie(w, &http.Cookie{
				Name:     "weaver_session",
				Value:    "session-token",
				Path:     "/",
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
			})
			_, _ = io.WriteString(w, "<!doctype html><html></html>")
		case "/graphql":
			graphqlRequests++
			cookie, err := r.Cookie("weaver_session")
			if err != nil || cookie.Value != "session-token" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			_, _ = io.WriteString(w, `{"data":{"version":"test"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := weaverHTTPClient(server.URL, time.Second)
	if err := refreshWeaverBrowserSession(client, server.URL+"/graphql"); err != nil {
		t.Fatalf("refresh browser session: %v", err)
	}

	resp, err := postGraphQLWithClient(client, server.URL+"/graphql", []byte(`{"query":"{ version }"}`))
	if err != nil {
		t.Fatalf("post graphql: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if uiLoads != 1 {
		t.Fatalf("ui loads = %d, want 1", uiLoads)
	}
	if graphqlRequests != 1 {
		t.Fatalf("graphql requests = %d, want 1", graphqlRequests)
	}
}

func TestGraphQLClientRefreshesStaleBrowserSessionCookie(t *testing.T) {
	var sawStale bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			http.SetCookie(w, &http.Cookie{
				Name:     "weaver_session",
				Value:    "fresh-token",
				Path:     "/",
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
			})
			_, _ = io.WriteString(w, "<!doctype html><html></html>")
		case "/graphql":
			cookie, err := r.Cookie("weaver_session")
			if err == nil && cookie.Value == "fresh-token" {
				_, _ = io.WriteString(w, `{"data":{"version":"test"}}`)
				return
			}
			if err == nil && cookie.Value == "stale-token" {
				sawStale = true
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := weaverHTTPClient(server.URL, time.Second)
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client.Jar.SetCookies(parsed, []*http.Cookie{{
		Name:  "weaver_session",
		Value: "stale-token",
		Path:  "/",
	}})

	resp, err := postGraphQLWithClient(client, server.URL+"/graphql", []byte(`{"query":"{ version }"}`))
	if err != nil {
		t.Fatalf("post graphql: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if !sawStale {
		t.Fatal("server did not observe the stale cookie before refresh")
	}
}
