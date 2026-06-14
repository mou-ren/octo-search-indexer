// Command verify drives end-to-end invariant checks against the local harness
// (Kafka + OpenSearch). It assumes the stack is up, the es-indexer is consuming,
// and the seed suite has been produced. It then asserts the C2/C4 + idempotency
// + IK-tokenization invariants by querying OpenSearch directly.
//
// ISOLATION: points only at the local harness endpoints (env ES / KAFKA_BROKERS).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	var (
		esURL   = flag.String("es", envOr("ES_URL", "http://localhost:19200"), "OpenSearch base URL")
		index   = flag.String("index", envOr("ES_INDEX", "octo-message"), "index name")
		waitFor = flag.Duration("wait", 60*time.Second, "max wait for docs to appear")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *waitFor+30*time.Second)
	defer cancel()

	v := &verifier{es: strings.TrimRight(*esURL, "/"), index: *index, hc: &http.Client{Timeout: 10 * time.Second}}

	// Refresh so seeded docs are searchable.
	if err := v.refresh(ctx); err != nil {
		log.Fatalf("refresh: %v", err)
	}

	var failures []string
	check := func(name string, err error) {
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", name, err))
			log.Printf("FAIL %s: %v", name, err)
		} else {
			log.Printf("PASS %s", name)
		}
	}

	// Positives first: these poll until present, proving the consumer has made
	// progress through the suite.
	// Invariant: idempotency — duplicate message_id => exactly one doc.
	check("idempotent-dup", v.expectDocExists(ctx, "m-dup"))
	check("idempotent-count", v.expectCount(ctx, term("message_id", "m-dup"), 1))

	// Invariant: raw_excluded doc IS indexed (content null), occupies a doc.
	check("raw-excluded-indexed", v.expectDocExists(ctx, "m-signal"))

	// Invariant: IK tokenization — Chinese query term recalls the doc.
	check("ik-recall-公园", v.expectMatchAtLeast(ctx, "content", "公园", 1))
	check("ik-recall-北京", v.expectMatchAtLeast(ctx, "content", "北京", 1))
	// English still works.
	check("en-recall-pipeline", v.expectMatchAtLeast(ctx, "content", "pipeline", 1))

	// Before negative assertions, the consumer MUST be proven drained: wait for the
	// index doc count to be unchanged for a quiet period. If drain can't be proven
	// (count never settles / _count errors), the "not indexed" assertions are
	// inconclusive — we record a FAILURE rather than continuing, so a poison pill
	// that would be indexed slightly later can never yield a false PASS.
	drainErr := v.waitDocCountStable(ctx, 8*time.Second, 40*time.Second)
	check("consumer-drained", drainErr)
	if drainErr == nil {
		// Invariant: schema_version unknown + bad JSON => NOT in ES (routed to DLQ).
		check("badschema-not-indexed", v.expectCount(ctx, term("message_id", "m-badschema"), 0))
		check("badjson-not-indexed", v.expectCount(ctx, term("message_id", "m-badjson"), 0))
	} else {
		// Drain unproven → do not assert "not indexed" (would be inconclusive).
		failures = append(failures, "badschema/badjson: skipped — consumer drain not proven")
		log.Printf("FAIL badschema/badjson: skipped — consumer drain not proven (%v)", drainErr)
	}

	if len(failures) > 0 {
		log.Fatalf("e2e verify FAILED (%d): %s", len(failures), strings.Join(failures, "; "))
	}
	log.Printf("e2e verify PASSED — all invariants hold")
}

type verifier struct {
	es    string
	index string
	hc    *http.Client
}

func (v *verifier) refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.es+"/"+v.index+"/_refresh", nil)
	if err != nil {
		return err
	}
	resp, err := v.hc.Do(req)
	if err != nil {
		return err
	}
	defer closeBody(resp)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("refresh status %d", resp.StatusCode)
	}
	return nil
}

// closeBody drains+closes a response body (errcheck-friendly helper).
func closeBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if cerr := resp.Body.Close(); cerr != nil {
		log.Printf("warn: close body: %v", cerr)
	}
}

// expectDocExists polls GET /<index>/_doc/<id> until found or timeout.
func (v *verifier) expectDocExists(ctx context.Context, id string) error {
	deadline := time.Now().Add(45 * time.Second)
	for {
		if rerr := v.refresh(ctx); rerr != nil {
			log.Printf("warn: refresh: %v", rerr)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.es+"/"+v.index+"/_doc/"+id, nil)
		if err != nil {
			return err
		}
		resp, err := v.hc.Do(req)
		if err == nil {
			code := resp.StatusCode
			closeBody(resp)
			if code == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("doc %q not found before deadline", id)
		}
		time.Sleep(2 * time.Second)
	}
}

func (v *verifier) expectCount(ctx context.Context, query map[string]any, want int) error {
	got, err := v.count(ctx, query)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("count=%d want=%d", got, want)
	}
	return nil
}

func (v *verifier) expectMatchAtLeast(ctx context.Context, field, q string, atLeast int) error {
	deadline := time.Now().Add(45 * time.Second)
	for {
		if rerr := v.refresh(ctx); rerr != nil {
			log.Printf("warn: refresh: %v", rerr)
		}
		got, err := v.count(ctx, match(field, q))
		if err == nil && got >= atLeast {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("match %q on %s returned %d (<%d)", q, field, got, atLeast)
		}
		time.Sleep(2 * time.Second)
	}
}

func (v *verifier) count(ctx context.Context, query map[string]any) (int, error) {
	body, err := json.Marshal(map[string]any{"query": query})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.es+"/"+v.index+"/_count", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer closeBody(resp)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("_count status %d", resp.StatusCode)
	}
	var out struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.Count, nil
}

// waitDocCountStable polls the total index doc count until it is unchanged for
// `quiet`, or `maxWait` elapses. Used to ensure the consumer has drained before
// asserting that poison-pill docs are NOT indexed (avoids a too-early false PASS).
func (v *verifier) waitDocCountStable(ctx context.Context, quiet, maxWait time.Duration) error {
	deadline := time.Now().Add(maxWait)
	matchAll := map[string]any{"match_all": map[string]any{}}
	last := -1
	stableSince := time.Now()
	for {
		if rerr := v.refresh(ctx); rerr != nil {
			log.Printf("warn: refresh: %v", rerr)
		}
		n, err := v.count(ctx, matchAll)
		if err != nil {
			return err
		}
		if n != last {
			last = n
			stableSince = time.Now()
		} else if time.Since(stableSince) >= quiet {
			return nil // count held steady for the quiet period
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("doc count not stable within %s (last=%d)", maxWait, last)
		}
		time.Sleep(1 * time.Second)
	}
}

func term(field, val string) map[string]any {
	return map[string]any{"term": map[string]any{field: val}}
}

func match(field, q string) map[string]any {
	return map[string]any{"match": map[string]any{field: q}}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
