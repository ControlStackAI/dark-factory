package linear

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/adapters/memory"
	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/factory"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

const (
	testToken   = "test-only-linear-token"
	testTeam    = "team-1"
	testProject = "project-1"
	readyID     = "state-ready"
	startedID   = "state-started"
	doneID      = "state-done"
)

type expectedRequest struct {
	operation string
	variables map[string]any
	response  string
}

type queueFake struct {
	t          *testing.T
	mu         sync.Mutex
	expected   []expectedRequest
	transcript []string
	server     *httptest.Server
}

func newQueueFake(t *testing.T, expected ...expectedRequest) *queueFake {
	t.Helper()
	f := &queueFake{t: t, expected: append([]expectedRequest(nil), expected...)}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.checkHeaders(r)
		var request graphQLRequest
		decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
		if err := decoder.Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(f.expected) == 0 {
			t.Errorf("unexpected operation %s", request.OperationName)
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		want := f.expected[0]
		f.expected = f.expected[1:]
		f.transcript = append(f.transcript, request.OperationName)
		if request.OperationName != want.operation || !strings.Contains(request.Query, request.OperationName) {
			t.Errorf("operation=%q query=%q want=%q", request.OperationName, request.Query, want.operation)
			http.Error(w, "unexpected operation", http.StatusBadRequest)
			return
		}
		if !reflect.DeepEqual(request.Variables, want.variables) {
			t.Errorf("%s variables=%#v want=%#v", request.OperationName, request.Variables, want.variables)
			http.Error(w, "unexpected variables", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, want.response)
	}))
	t.Cleanup(func() {
		f.server.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(f.expected) != 0 {
			t.Errorf("%d expected GraphQL operations were not received; next=%s", len(f.expected), f.expected[0].operation)
		}
	})
	return f
}

func (f *queueFake) checkHeaders(r *http.Request) {
	f.t.Helper()
	if r.Method != http.MethodPost || r.Header.Get("Authorization") != testToken ||
		r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json" ||
		r.Header.Get("User-Agent") != "dark-factory/0.1" {
		f.t.Errorf("unexpected request headers/method: method=%s auth-match=%t content=%q accept=%q user-agent=%q",
			r.Method, r.Header.Get("Authorization") == testToken, r.Header.Get("Content-Type"), r.Header.Get("Accept"), r.Header.Get("User-Agent"))
	}
}

func testClient(t *testing.T, endpoint string, client *http.Client, overrides func(*Options)) *Client {
	t.Helper()
	options := Options{Endpoint: endpoint, APIKey: testToken, TeamID: testTeam, ProjectID: testProject,
		ReadyName: "Ready", InProgressName: "In Progress", DoneName: "Done", HTTPClient: client,
		RequestTimeout: 250 * time.Millisecond, MaxResponseBytes: 1 << 20, MaxPages: 10, PageSize: 2,
		MaxRetries: 2, InitialBackoff: time.Millisecond, MaxRetryAfter: 10 * time.Millisecond,
		Sleep: func(ctx context.Context, _ time.Duration) error { return ctx.Err() }}
	if overrides != nil {
		overrides(&options)
	}
	result, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func statesResponse(states []map[string]any) string {
	return statesPage(states, false, "")
}

func statesPage(states []map[string]any, more bool, cursor string) string {
	return response(map[string]any{"team": map[string]any{"id": testTeam, "states": map[string]any{"nodes": states, "pageInfo": map[string]any{"hasNextPage": more, "endCursor": cursor}}}})
}

func standardStates() []map[string]any {
	return []map[string]any{
		{"id": readyID, "name": "Ready", "type": "unstarted", "position": 2},
		{"id": startedID, "name": "In Progress", "type": "started", "position": 3},
		{"id": doneID, "name": "Done", "type": "completed", "position": 4},
	}
}

func issueNode(id, identifier, stateID, stateName, stateType, created string, priority int) map[string]any {
	return map[string]any{"id": id, "identifier": identifier, "title": identifier + " title", "priority": priority,
		"createdAt": created, "project": map[string]any{"id": testProject}, "team": map[string]any{"id": testTeam},
		"state": map[string]any{"id": stateID, "name": stateName, "type": stateType}}
}

func response(data any) string {
	b, _ := json.Marshal(map[string]any{"data": data})
	return string(b)
}

func TestSelectReadyCompletelyPaginatesAndOrders(t *testing.T) {
	created := "2026-08-21T10:00:00Z"
	current := issueNode("id-current", "DF-0", readyID, "Ready", "unstarted", created, 1)
	blocked := issueNode("id-blocked", "DF-1", readyID, "Ready", "unstarted", "2026-08-20T10:00:00Z", 1)
	unset := issueNode("id-unset", "DF-9", readyID, "Ready", "unstarted", "2026-08-19T10:00:00Z", 0)
	tieB := issueNode("id-b", "DF-3", readyID, "Ready", "unstarted", created, 1)
	tieA := issueNode("id-a", "DF-2", readyID, "Ready", "unstarted", created, 1)
	f := newQueueFake(t,
		expectedRequest{"DarkFactoryStates", map[string]any{"teamId": testTeam, "first": float64(2), "after": nil}, statesResponse(standardStates())},
		expectedRequest{"DarkFactoryIssues", map[string]any{"projectId": testProject, "first": float64(2), "after": nil}, issuePage([]map[string]any{current, blocked}, true, "page-1")},
		expectedRequest{"DarkFactoryIssues", map[string]any{"projectId": testProject, "first": float64(2), "after": "page-1"}, issuePage([]map[string]any{unset, tieB}, true, "page-2")},
		expectedRequest{"DarkFactoryIssues", map[string]any{"projectId": testProject, "first": float64(2), "after": "page-2"}, issuePage([]map[string]any{tieA}, false, "")},
		expectedRequest{"DarkFactoryRelations", map[string]any{"id": "id-blocked", "first": float64(2), "after": nil}, relationPage(blocked, nil, true, "relations-1")},
		expectedRequest{"DarkFactoryRelations", map[string]any{"id": "id-blocked", "first": float64(2), "after": "relations-1"}, relationPage(blocked, []map[string]any{
			{"type": "blocked_by", "relatedIssue": map[string]any{"id": "blocker", "identifier": "DF-X", "state": map[string]any{"id": startedID, "name": "In Progress", "type": "started"}}},
		}, false, "")},
		expectedRequest{"DarkFactoryRelations", map[string]any{"id": "id-unset", "first": float64(2), "after": nil}, relationPage(unset, nil, false, "")},
		expectedRequest{"DarkFactoryRelations", map[string]any{"id": "id-b", "first": float64(2), "after": nil}, relationPage(tieB, nil, false, "")},
		expectedRequest{"DarkFactoryRelations", map[string]any{"id": "id-a", "first": float64(2), "after": nil}, relationPage(tieA, nil, false, "")},
	)
	client := testClient(t, f.server.URL, f.server.Client(), nil)
	selected, found, err := client.SelectReady(context.Background(), "DF-0")
	if err != nil || !found || selected.Identifier != "DF-2" {
		t.Fatalf("selected=%+v found=%t err=%v", selected, found, err)
	}
	t.Logf("strict transcript=%v selected=%s priority=%d", f.transcript, selected.Identifier, selected.Priority)
}

func TestSelectionNeverInfersFromFirstHundred(t *testing.T) {
	var firstHundred []map[string]any
	for index := 0; index < 100; index++ {
		firstHundred = append(firstHundred, issueNode(fmt.Sprintf("done-%03d", index), fmt.Sprintf("DF-%03d", index), doneID, "Done", "completed", "2026-08-20T10:00:00Z", 1))
	}
	winner := issueNode("winner", "DF-999", readyID, "Ready", "unstarted", "2026-08-21T10:00:00Z", 1)
	f := newQueueFake(t,
		expectedRequest{"DarkFactoryStates", map[string]any{"teamId": testTeam, "first": float64(100), "after": nil}, statesResponse(standardStates())},
		expectedRequest{"DarkFactoryIssues", map[string]any{"projectId": testProject, "first": float64(100), "after": nil}, issuePage(firstHundred, true, "hundred")},
		expectedRequest{"DarkFactoryIssues", map[string]any{"projectId": testProject, "first": float64(100), "after": "hundred"}, issuePage([]map[string]any{winner}, false, "")},
		expectedRequest{"DarkFactoryRelations", map[string]any{"id": "winner", "first": float64(100), "after": nil}, relationPage(winner, nil, false, "")},
	)
	client := testClient(t, f.server.URL, f.server.Client(), func(o *Options) { o.PageSize = 100 })
	issue, found, err := client.SelectReady(context.Background(), "current")
	if err != nil || !found || issue.ID != "winner" {
		t.Fatalf("issue=%+v found=%t err=%v", issue, found, err)
	}
}

func issuePage(nodes []map[string]any, more bool, cursor string) string {
	return response(map[string]any{"project": map[string]any{"id": testProject, "issues": map[string]any{"nodes": nodes, "pageInfo": map[string]any{"hasNextPage": more, "endCursor": cursor}}}})
}

func relationPage(issue map[string]any, nodes []map[string]any, more bool, cursor string) string {
	return response(map[string]any{"issue": map[string]any{"id": issue["id"], "identifier": issue["identifier"], "project": issue["project"], "team": issue["team"],
		"relations": map[string]any{"nodes": nodes, "pageInfo": map[string]any{"hasNextPage": more, "endCursor": cursor}}}})
}

func TestScopeAllowlistAndCurrentExclusion(t *testing.T) {
	created := "2026-08-21T10:00:00Z"
	for _, tc := range []struct {
		name         string
		project      string
		team         string
		allowlist    []string
		requestID    string
		wantConflict bool
	}{
		{"wrong project", "other", testTeam, nil, "id-1", true},
		{"wrong team", testProject, "other", nil, "id-1", true},
		{"allowlist preflight", testProject, testTeam, []string{"id-allowed"}, "id-1", true},
		{"allowed", testProject, testTeam, []string{"id-1"}, "id-1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := issueNode("id-1", "DF-1", readyID, "Ready", "unstarted", created, 1)
			node["project"] = map[string]any{"id": tc.project}
			node["team"] = map[string]any{"id": tc.team}
			var f *queueFake
			if tc.name != "allowlist preflight" {
				f = newQueueFake(t, expectedRequest{"DarkFactoryIssue", map[string]any{"id": tc.requestID}, response(map[string]any{"issue": node})})
			} else {
				f = newQueueFake(t)
			}
			client := testClient(t, f.server.URL, f.server.Client(), func(o *Options) { o.IssueAllowlist = tc.allowlist })
			_, err := client.GetIssue(context.Background(), tc.requestID)
			if tc.wantConflict != errors.Is(err, ports.ErrConflict) {
				t.Fatalf("error=%v want conflict=%t", err, tc.wantConflict)
			}
		})
	}
}

func TestIssueQueryCannotRedirectFrozenReference(t *testing.T) {
	node := issueNode("different-id", "DF-OTHER", readyID, "Ready", "unstarted", "2026-08-21T10:00:00Z", 1)
	f := newQueueFake(t, expectedRequest{
		"DarkFactoryIssue",
		map[string]any{"id": "requested-id"},
		response(map[string]any{"issue": node}),
	})
	client := testClient(t, f.server.URL, f.server.Client(), nil)
	if _, err := client.GetIssue(context.Background(), "requested-id"); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("redirected issue query error=%v", err)
	}
}

func TestLifecycleResolutionMissingAndAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		name      string
		states    []remoteState
		typeName  string
		preferred string
		wantID    string
		wantErr   bool
	}{
		{"missing", nil, "started", "In Progress", "", true},
		{"ambiguous", []remoteState{{ID: "a", Name: "Doing", Type: "started"}, {ID: "b", Name: "WIP", Type: "started"}}, "started", "In Progress", "", true},
		{"preferred tie break", []remoteState{{ID: "a", Name: "Doing", Type: "started"}, {ID: "b", Name: "In Progress", Type: "started"}}, "started", "In Progress", "b", false},
		{"type not name", []remoteState{{ID: "a", Name: "Ready", Type: "started"}}, "started", "In Progress", "a", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := chooseState(tc.states, tc.typeName, tc.preferred)
			if (err != nil) != tc.wantErr || got.ID != tc.wantID {
				t.Fatalf("state=%+v err=%v", got, err)
			}
		})
	}
}

func TestLifecycleAndCommentConnectionsCompletelyPaginate(t *testing.T) {
	f := newQueueFake(t,
		expectedRequest{"DarkFactoryStates", map[string]any{"teamId": testTeam, "first": float64(2), "after": nil}, statesPage(standardStates()[:1], true, "states-1")},
		expectedRequest{"DarkFactoryStates", map[string]any{"teamId": testTeam, "first": float64(2), "after": "states-1"}, statesPage(standardStates()[1:], false, "")},
		expectedRequest{"DarkFactoryComments", map[string]any{"id": "current", "first": float64(2), "after": nil}, commentPage("current", []string{"first"}, true, "comments-1")},
		expectedRequest{"DarkFactoryComments", map[string]any{"id": "current", "first": float64(2), "after": "comments-1"}, commentPage("current", []string{"second"}, false, "")},
	)
	client := testClient(t, f.server.URL, f.server.Client(), nil)
	states, err := client.resolveLifecycle(context.Background())
	if err != nil || states.ready.ID != readyID || states.inProgress.ID != startedID || states.done.ID != doneID {
		t.Fatalf("states=%+v err=%v", states, err)
	}
	issue := remoteIssue{ID: "current", Identifier: "DF-1", Project: &struct {
		ID string `json:"id"`
	}{ID: testProject}}
	issue.Team.ID = testTeam
	bodies, err := client.listComments(context.Background(), issue)
	if err != nil || !reflect.DeepEqual(bodies, []string{"first", "second"}) {
		t.Fatalf("comments=%v err=%v", bodies, err)
	}
}

func commentPage(issueID string, bodies []string, more bool, cursor string) string {
	var nodes []map[string]any
	for index, body := range bodies {
		nodes = append(nodes, map[string]any{"id": fmt.Sprintf("comment-%d", index), "body": body})
	}
	return response(map[string]any{"issue": map[string]any{
		"id": issueID, "identifier": "DF-1", "project": map[string]any{"id": testProject}, "team": map[string]any{"id": testTeam},
		"comments": map[string]any{"nodes": nodes, "pageInfo": map[string]any{"hasNextPage": more, "endCursor": cursor}},
	}})
}

func TestPaginationRequestAndMarkerBoundsFailClosed(t *testing.T) {
	f := newQueueFake(t,
		expectedRequest{"DarkFactoryStates", map[string]any{"teamId": testTeam, "first": float64(2), "after": nil}, statesResponse(standardStates())},
		expectedRequest{"DarkFactoryIssues", map[string]any{"projectId": testProject, "first": float64(2), "after": nil}, issuePage(nil, true, "more")},
	)
	client := testClient(t, f.server.URL, f.server.Client(), func(o *Options) { o.MaxPages = 1 })
	if _, _, err := client.SelectReady(context.Background(), "current"); err == nil || !strings.Contains(err.Error(), "pagination exceeded") {
		t.Fatalf("pagination error=%v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("oversized request reached server") }))
	defer server.Close()
	client = testClient(t, server.URL, server.Client(), func(o *Options) { o.MaxRequestBytes = 200 })
	if err := client.query(context.Background(), "DarkFactoryOversized", "query DarkFactoryOversized($value:String!){x}", map[string]any{"value": strings.Repeat("x", 500)}, &struct{}{}); err == nil || !strings.Contains(err.Error(), "request exceeds") {
		t.Fatalf("request bound error=%v", err)
	}

	intended := commentIntent{Version: 1, Key: "key", RunID: "run", ProjectID: testProject, IssueID: "issue", Transition: "completed"}
	if _, err := inspectComments([]string{"<!-- dark-factory:{not-json} -->"}, intended); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("malformed marker error=%v", err)
	}
}

type fakeRecord struct {
	ID, Identifier, Title, CreatedAt string
	Priority                         int
	StateID, StateName, StateType    string
}

type stateFake struct {
	t           *testing.T
	mu          sync.Mutex
	issues      map[string]*fakeRecord
	comments    map[string][]string
	blocked     map[string]bool
	transcript  []string
	mutations   map[string]int
	faultKey    string
	faultBefore bool
	faultUsed   bool
	cancel      context.CancelFunc
	server      *httptest.Server
}

func newStateFake(t *testing.T) *stateFake {
	t.Helper()
	f := &stateFake{t: t, issues: map[string]*fakeRecord{}, comments: map[string][]string{}, blocked: map[string]bool{}, mutations: map[string]int{}}
	f.issues["current"] = &fakeRecord{ID: "current", Identifier: "DF-1", Title: "current", CreatedAt: "2026-08-20T10:00:00Z", Priority: 1, StateID: startedID, StateName: "In Progress", StateType: "started"}
	f.issues["next"] = &fakeRecord{ID: "next", Identifier: "DF-2", Title: "next", CreatedAt: "2026-08-21T10:00:00Z", Priority: 2, StateID: readyID, StateName: "Ready", StateType: "unstarted"}
	f.issues["other"] = &fakeRecord{ID: "other", Identifier: "DF-3", Title: "other", CreatedAt: "2026-08-22T10:00:00Z", Priority: 3, StateID: readyID, StateName: "Ready", StateType: "unstarted"}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *stateFake) handle(w http.ResponseWriter, r *http.Request) {
	(&queueFake{t: f.t}).checkHeaders(r)
	var request graphQLRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&request); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transcript = append(f.transcript, request.OperationName)
	w.Header().Set("Content-Type", "application/json")
	switch request.OperationName {
	case "DarkFactoryStates":
		f.requireVars(request, "teamId", "first", "after")
		_, _ = io.WriteString(w, statesResponse(standardStates()))
	case "DarkFactoryIssue":
		f.requireVars(request, "id")
		id, _ := request.Variables["id"].(string)
		record := f.find(id)
		_, _ = io.WriteString(w, response(map[string]any{"issue": f.node(record)}))
	case "DarkFactoryIssues":
		f.requireVars(request, "projectId", "first", "after")
		var ids []string
		for id := range f.issues {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		var nodes []map[string]any
		for _, id := range ids {
			nodes = append(nodes, f.node(f.issues[id]))
		}
		_, _ = io.WriteString(w, issuePage(nodes, false, ""))
	case "DarkFactoryRelations":
		f.requireVars(request, "id", "first", "after")
		id := request.Variables["id"].(string)
		var nodes []map[string]any
		if f.blocked[id] {
			nodes = append(nodes, map[string]any{
				"type": "blocked_by",
				"relatedIssue": map[string]any{"id": "blocker", "identifier": "DF-BLOCKER", "state": map[string]any{
					"id": startedID, "name": "In Progress", "type": "started",
				}},
			})
		}
		_, _ = io.WriteString(w, relationPage(f.node(f.find(id)), nodes, false, ""))
	case "DarkFactoryComments":
		f.requireVars(request, "id", "first", "after")
		id := request.Variables["id"].(string)
		var nodes []map[string]any
		for index, body := range f.comments[id] {
			nodes = append(nodes, map[string]any{"id": fmt.Sprintf("c%d", index+1), "body": body})
		}
		node := f.node(f.find(id))
		_, _ = io.WriteString(w, response(map[string]any{"issue": map[string]any{"id": node["id"], "identifier": node["identifier"], "project": node["project"], "team": node["team"], "comments": map[string]any{"nodes": nodes, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""}}}}))
	case "DarkFactoryUpdateIssue":
		f.requireVars(request, "id", "input")
		id := request.Variables["id"].(string)
		input, ok := request.Variables["input"].(map[string]any)
		if !ok || len(input) != 1 {
			f.t.Errorf("invalid update input %#v", request.Variables["input"])
			http.Error(w, "bad input", 400)
			return
		}
		stateID, _ := input["stateId"].(string)
		key := "update:" + id + ":" + stateID
		if f.triggerFault(w, r, key, func() { f.setState(id, stateID); f.mutations[key]++ }) {
			return
		}
		f.setState(id, stateID)
		f.mutations[key]++
		_, _ = io.WriteString(w, response(map[string]any{"issueUpdate": map[string]any{"success": true, "issue": f.node(f.find(id))}}))
	case "DarkFactoryCreateComment":
		f.requireVars(request, "input")
		input, ok := request.Variables["input"].(map[string]any)
		if !ok || len(input) != 2 {
			f.t.Errorf("invalid comment input %#v", request.Variables["input"])
			http.Error(w, "bad input", 400)
			return
		}
		id, _ := input["issueId"].(string)
		body, _ := input["body"].(string)
		intent, ok := parseMarker(body)
		if !ok || intent.ProjectID != testProject || intent.IssueID != id {
			f.t.Errorf("invalid comment marker/body")
			http.Error(w, "bad marker", 400)
			return
		}
		key := "comment:" + id + ":" + intent.Transition
		commit := func() { f.comments[id] = append(f.comments[id], body); f.mutations[key]++ }
		if f.triggerFault(w, r, key, commit) {
			return
		}
		commit()
		_, _ = io.WriteString(w, response(map[string]any{"commentCreate": map[string]any{"success": true, "comment": map[string]any{"id": fmt.Sprintf("c%d", len(f.comments[id]))}}}))
	default:
		f.t.Errorf("unexpected GraphQL operation %q", request.OperationName)
		http.Error(w, "unexpected operation", 400)
	}
}

func (f *stateFake) requireVars(request graphQLRequest, keys ...string) {
	f.t.Helper()
	got := make([]string, 0, len(request.Variables))
	for key := range request.Variables {
		got = append(got, key)
	}
	sort.Strings(got)
	sort.Strings(keys)
	if !reflect.DeepEqual(got, keys) {
		f.t.Errorf("%s variable keys=%v want=%v", request.OperationName, got, keys)
	}
	if team, ok := request.Variables["teamId"]; ok && team != testTeam {
		f.t.Errorf("wrong team variable %#v", team)
	}
	if project, ok := request.Variables["projectId"]; ok && project != testProject {
		f.t.Errorf("wrong project variable %#v", project)
	}
}

func (f *stateFake) triggerFault(w http.ResponseWriter, r *http.Request, key string, commit func()) bool {
	if f.faultUsed || f.faultKey != key {
		return false
	}
	f.faultUsed = true
	if !f.faultBefore {
		commit()
	}
	if f.cancel != nil {
		f.cancel()
	}
	<-r.Context().Done()
	return true
}

func (f *stateFake) find(reference string) *fakeRecord {
	if record := f.issues[reference]; record != nil {
		return record
	}
	for _, record := range f.issues {
		if record.Identifier == reference {
			return record
		}
	}
	return nil
}

func (f *stateFake) node(record *fakeRecord) map[string]any {
	if record == nil {
		return nil
	}
	return issueNode(record.ID, record.Identifier, record.StateID, record.StateName, record.StateType, record.CreatedAt, record.Priority)
}

func (f *stateFake) setState(id, state string) {
	record := f.find(id)
	switch state {
	case readyID:
		record.StateID, record.StateName, record.StateType = readyID, "Ready", "unstarted"
	case startedID:
		record.StateID, record.StateName, record.StateType = startedID, "In Progress", "started"
	case doneID:
		record.StateID, record.StateName, record.StateType = doneID, "Done", "completed"
	default:
		f.t.Errorf("unexpected state %q", state)
	}
}

func (f *stateFake) client(t *testing.T) *Client {
	return testClient(t, f.server.URL, f.server.Client(), func(o *Options) { o.PageSize = 100; o.RequestTimeout = 100 * time.Millisecond })
}

func TestClaimIdempotentAndTimeoutReconciliation(t *testing.T) {
	for _, tc := range []struct {
		name, fault string
		before      bool
	}{
		{"idempotent", "", false},
		{"timeout before commit", "comment:next:started", true},
		{"timeout after committed claim comment", "comment:next:started", false},
		{"timeout after committed claim", "update:next:" + startedID, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStateFake(t)
			f.faultKey, f.faultBefore = tc.fault, tc.before
			ctx := context.Background()
			if tc.fault != "" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				f.cancel = cancel
			}
			request := ClaimRequest{RunID: "run-1", IssueID: "next", IdempotencyKey: "claim-key"}
			err := f.client(t).Claim(ctx, request)
			if tc.fault != "" && err == nil {
				t.Fatal("faulted claim unexpectedly succeeded")
			}
			if err := f.client(t).Claim(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if err := f.client(t).Claim(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.issues["next"].StateID != startedID || f.issues["other"].StateID != readyID || f.mutations["update:next:"+startedID] != 1 ||
				f.mutations["comment:next:started"] != 1 || len(f.comments["next"]) != 1 {
				t.Fatalf("state=%s mutations=%v", f.issues["next"].StateID, f.mutations)
			}
		})
	}
}

func TestMutationAllowlistPreflightRefusesWithoutNetwork(t *testing.T) {
	f := newQueueFake(t)
	client := testClient(t, f.server.URL, f.server.Client(), func(o *Options) { o.IssueAllowlist = []string{"current"} })
	if err := client.Claim(context.Background(), ClaimRequest{RunID: "run", IssueID: "next", IdempotencyKey: "key"}); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("claim allowlist error=%v", err)
	}
	if err := client.Advance(context.Background(), advanceRequest()); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("advance allowlist error=%v", err)
	}
}

func TestFrozenClaimAndAdvancementRefuseNewBlockerBeforeMutation(t *testing.T) {
	for _, operation := range []string{"claim", "advance"} {
		t.Run(operation, func(t *testing.T) {
			f := newStateFake(t)
			f.blocked["next"] = true
			client := f.client(t)
			var err error
			if operation == "claim" {
				err = client.Claim(context.Background(), ClaimRequest{RunID: "run", IssueID: "next", IdempotencyKey: "key"})
			} else {
				err = client.Advance(context.Background(), advanceRequest())
			}
			if !errors.Is(err, ports.ErrInvalidTransition) {
				t.Fatalf("blocked %s error=%v", operation, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.mutations) != 0 || len(f.comments["next"]) != 0 || len(f.comments["current"]) != 0 {
				t.Fatalf("blocked %s mutated remote state: mutations=%v comments=%v", operation, f.mutations, f.comments)
			}
		})
	}
}

func TestClaimConflictingKeyAndUnattributedStartedStateRefused(t *testing.T) {
	f := newStateFake(t)
	client := f.client(t)
	request := ClaimRequest{RunID: "run-claim", IssueID: "next", IdempotencyKey: "claim-key"}
	if err := client.Claim(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.IdempotencyKey = "different-key"
	if err := client.Claim(context.Background(), request); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("conflicting claim error=%v", err)
	}

	f2 := newStateFake(t)
	f2.setState("next", startedID)
	if err := f2.client(t).Claim(context.Background(), ClaimRequest{RunID: "run", IssueID: "next", IdempotencyKey: "key"}); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("unattributed started state error=%v", err)
	}
}

func advanceRequest() domain.AdvanceRequest {
	return domain.AdvanceRequest{RunID: "run-1", ProjectID: testProject, CurrentIssueID: "current", NextIssueID: "next", Evidence: "tests passed", ReviewID: "review-1", Fence: 1, IdempotencyKey: "advance-key"}
}

func TestAdvancementRestartMatrixEveryCommittedSuboperation(t *testing.T) {
	faults := []string{"comment:current:completed", "update:current:" + doneID, "comment:next:started", "update:next:" + startedID}
	for _, fault := range faults {
		t.Run(fault, func(t *testing.T) {
			f := newStateFake(t)
			f.faultKey = fault
			ctx, cancel := context.WithCancel(context.Background())
			f.cancel = cancel
			localReceipts := 0
			if err := f.client(t).Advance(ctx, advanceRequest()); err == nil {
				localReceipts++
			}
			if localReceipts != 0 {
				t.Fatal("local receipt written before verified remote success")
			}
			if err := f.client(t).Advance(context.Background(), advanceRequest()); err != nil {
				t.Fatal(err)
			}
			localReceipts++
			if err := f.client(t).Advance(context.Background(), advanceRequest()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.issues["current"].StateID != doneID || f.issues["next"].StateID != startedID || f.issues["other"].StateID != readyID {
				t.Fatalf("unexpected final states: %#v", f.issues)
			}
			if len(f.comments["current"]) != 1 || len(f.comments["next"]) != 1 || localReceipts != 1 {
				t.Fatalf("comments=%v receipts=%d", f.comments, localReceipts)
			}
			for _, key := range []string{"comment:current:completed", "update:current:" + doneID, "comment:next:started", "update:next:" + startedID} {
				if f.mutations[key] != 1 {
					t.Fatalf("mutation %s count=%d all=%v", key, f.mutations[key], f.mutations)
				}
			}
			t.Logf("fault=%s transcript=%v mutation-cardinality=%v local-receipts=%d", fault, f.transcript, f.mutations, localReceipts)
			var mutationOrder []string
			for _, operation := range f.transcript {
				if operation == "DarkFactoryCreateComment" || operation == "DarkFactoryUpdateIssue" {
					mutationOrder = append(mutationOrder, operation)
				}
			}
			wantOrder := []string{"DarkFactoryCreateComment", "DarkFactoryUpdateIssue", "DarkFactoryCreateComment", "DarkFactoryUpdateIssue"}
			if !reflect.DeepEqual(mutationOrder, wantOrder) {
				t.Fatalf("mutation order=%v want=%v", mutationOrder, wantOrder)
			}
		})
	}
}

func TestCommittedRemoteAdvanceReconcilesOneDurableLocalReceiptAfterControllerRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "factory.db")
	store, err := durablesqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	seededAt := time.Now().UTC().Add(-time.Hour)
	for _, issue := range []domain.Issue{
		{ID: "current", ProjectID: testProject, Title: "current", Priority: 1, CreatedAt: seededAt, State: domain.IssueInProgress},
		{ID: "next", ProjectID: testProject, Title: "next", Priority: 2, CreatedAt: seededAt.Add(time.Minute), State: domain.IssueReady},
	} {
		if err := store.EnsureIssue(ctx, issue); err != nil {
			t.Fatal(err)
		}
	}
	digest, err := store.EnsureArtifact(ctx, "sqlite://linear-review", []byte("immutable Linear integration review\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureReview(ctx, domain.ReviewEvidence{
		ID: "review-1", ProjectID: testProject, IssueID: "current", Status: domain.ReviewApproved,
		Immutable: true, ArtifactRef: "sqlite://linear-review", ArtifactSHA256: digest,
	}); err != nil {
		t.Fatal(err)
	}
	f := newStateFake(t)
	controller := factory.New(store, f.client(t), memory.NewOpenClaw(), store, store)
	policy := domain.Policy{LeaseDuration: time.Minute, MaxRunDuration: time.Hour, MaxAttempts: 3, MaxConsecutiveFailures: 2}
	if _, err := controller.Start(ctx, "run-1", testProject, "current", "start Linear integration", policy); err != nil {
		t.Fatal(err)
	}
	lease, err := controller.AcquireLease(ctx, "run-1", "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.BindReview(ctx, "run-1", lease.Fence, "review-1"); err != nil {
		t.Fatal(err)
	}

	f.faultKey = "update:next:" + startedID
	faultCtx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	if _, err := controller.CompleteAndAdvance(faultCtx, "run-1", lease.Fence, "reviewed Linear integration passed"); err == nil {
		t.Fatal("ambiguous committed remote advancement unexpectedly wrote local success")
	}
	pending, err := store.Get(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if pending.PendingAdvance == nil || pending.IssueID != "current" {
		t.Fatalf("ambiguous remote commit did not preserve frozen local intent: %+v", pending)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = durablesqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	controller = factory.New(store, f.client(t), memory.NewOpenClaw(), store, store)
	reconciled, err := controller.CompleteAndAdvance(ctx, "run-1", lease.Fence, "ignored; frozen evidence is authoritative")
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.PendingAdvance != nil || reconciled.IssueID != "next" {
		t.Fatalf("durable reconciliation did not adopt the frozen issue: %+v", reconciled)
	}
	journal, err := store.Journal(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	phaseCounts := map[string]int{}
	for _, entry := range journal {
		phaseCounts[entry.Phase]++
	}
	if phaseCounts["advance_frozen"] != 1 || phaseCounts["advance_reconciled"] != 1 {
		t.Fatalf("durable local receipt cardinality=%v", phaseCounts)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = durablesqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reopened, err := store.Get(ctx, "run-1")
	if err != nil || reopened.PendingAdvance != nil || reopened.IssueID != "next" {
		t.Fatalf("reopened durable receipt run=%+v err=%v", reopened, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range []string{"comment:current:completed", "update:current:" + doneID, "comment:next:started", "update:next:" + startedID} {
		if f.mutations[key] != 1 {
			t.Fatalf("remote mutation %s cardinality=%d all=%v", key, f.mutations[key], f.mutations)
		}
	}
	if f.issues["other"].StateID != readyID {
		t.Fatalf("unfrozen issue was mutated: %+v", f.issues["other"])
	}
}

func TestAdvancementTimeoutBeforeCommitRetriesFrozenIntent(t *testing.T) {
	f := newStateFake(t)
	f.faultKey, f.faultBefore = "comment:current:completed", true
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	if err := f.client(t).Advance(ctx, advanceRequest()); err == nil {
		t.Fatal("timeout before commit unexpectedly succeeded")
	}
	if err := f.client(t).Advance(context.Background(), advanceRequest()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.comments["current"]) != 1 || len(f.comments["next"]) != 1 {
		t.Fatalf("comments=%v", f.comments)
	}
}

func TestAdvanceWithoutNextCreatesOnlyCompletionComment(t *testing.T) {
	f := newStateFake(t)
	request := advanceRequest()
	request.NextIssueID = ""
	if err := f.client(t).Advance(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.comments["current"]) != 1 || len(f.comments["next"]) != 0 || f.issues["next"].StateID != readyID {
		t.Fatalf("comments=%v next=%s", f.comments, f.issues["next"].StateID)
	}
	if strings.Contains(f.comments["current"][0], request.Evidence) {
		t.Fatal("raw review evidence was transmitted in the Linear comment")
	}
}

func TestAlreadyTransitionedWithoutMatchingKeyIsNotReplaySuccess(t *testing.T) {
	f := newStateFake(t)
	f.setState("current", doneID)
	if err := f.client(t).Advance(context.Background(), advanceRequest()); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("error=%v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.comments["current"]) != 0 || len(f.mutations) != 0 {
		t.Fatalf("unexpected mutation comments=%v mutations=%v", f.comments, f.mutations)
	}
}

func TestAdvanceConcurrentReplayHasSingleMutationCardinality(t *testing.T) {
	f := newStateFake(t)
	client := f.client(t)
	request := advanceRequest()
	const callers = 12
	errorsOut := make(chan error, callers)
	var group sync.WaitGroup
	for i := 0; i < callers; i++ {
		group.Add(1)
		go func() { defer group.Done(); errorsOut <- client.Advance(context.Background(), request) }()
	}
	group.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, count := range f.mutations {
		if count != 1 {
			t.Fatalf("mutation cardinality=%v", f.mutations)
		}
	}
}

func TestDuplicateKeyReplayAndConflictingKeyRefusal(t *testing.T) {
	f := newStateFake(t)
	client := f.client(t)
	if err := client.Advance(context.Background(), advanceRequest()); err != nil {
		t.Fatal(err)
	}
	if err := client.Advance(context.Background(), advanceRequest()); err != nil {
		t.Fatal(err)
	}
	conflict := advanceRequest()
	conflict.IdempotencyKey = "other-key"
	if err := client.Advance(context.Background(), conflict); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("conflicting key error=%v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.comments["current"]) != 1 || len(f.comments["next"]) != 1 {
		t.Fatalf("duplicate comments: %v", f.comments)
	}
}

func TestTransportBoundsRetriesAndRedaction(t *testing.T) {
	secret := "auth-secret-must-not-leak"
	for _, tc := range []struct {
		name     string
		handler  http.HandlerFunc
		maxBytes int64
		want     string
	}{
		{"malformed JSON", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{`) }, 1024, "malformed JSON"},
		{"oversized JSON", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, strings.Repeat("x", 1025)) }, 1024, "exceeds"},
		{"GraphQL errors redacted", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"errors":[{"message":"`+secret+` variable secret"}],"data":null}`)
		}, 1024, "details redacted"},
		{"auth redacted", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401); _, _ = io.WriteString(w, secret) }, 1024, "authentication failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			client := testClient(t, server.URL, server.Client(), func(o *Options) { o.APIKey = secret; o.MaxResponseBytes = tc.maxBytes; o.MaxRetries = 1 })
			err := client.Probe(context.Background(), "current")
			if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), secret) {
				t.Fatalf("error=%q", err)
			}
		})
	}

	var mu sync.Mutex
	attempts := 0
	var delays []time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts < 3 {
			w.Header().Set("Retry-After", "99")
			w.WriteHeader(429)
			return
		}
		_, _ = io.WriteString(w, statesResponse(standardStates()))
	}))
	defer server.Close()
	client := testClient(t, server.URL, server.Client(), func(o *Options) {
		o.MaxRetries = 2
		o.MaxRetryAfter = 7 * time.Millisecond
		o.Sleep = func(_ context.Context, d time.Duration) error { delays = append(delays, d); return nil }
	})
	if _, err := client.resolveLifecycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || !reflect.DeepEqual(delays, []time.Duration{7 * time.Millisecond, 7 * time.Millisecond}) {
		t.Fatalf("attempts=%d delays=%v", attempts, delays)
	}

	mutationAttempts := 0
	server429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != testToken {
			t.Error("mutation retry request lost authorization header")
		}
		mutationAttempts++
		if mutationAttempts < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, response(map[string]any{"ok": true}))
	}))
	defer server429.Close()
	mutationClient := testClient(t, server429.URL, server429.Client(), func(o *Options) {
		o.MaxRetries = 2
		o.Sleep = func(context.Context, time.Duration) error { return nil }
	})
	var mutationResult struct {
		OK bool `json:"ok"`
	}
	if err := mutationClient.mutate(context.Background(), "DarkFactoryRateLimitTest", "mutation DarkFactoryRateLimitTest{ok}", map[string]any{}, &mutationResult); err != nil {
		t.Fatal(err)
	}
	if mutationAttempts != 3 || !mutationResult.OK {
		t.Fatalf("mutation attempts=%d result=%+v", mutationAttempts, mutationResult)
	}
}

func TestTimeoutIsBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(60 * time.Millisecond)
		_, _ = io.WriteString(w, statesResponse(standardStates()))
	}))
	defer server.Close()
	client := testClient(t, server.URL, server.Client(), func(o *Options) { o.RequestTimeout = 20 * time.Millisecond; o.MaxRetries = 1 })
	started := time.Now()
	err := client.Probe(context.Background(), "current")
	if err == nil || time.Since(started) > 250*time.Millisecond {
		t.Fatalf("err=%v elapsed=%s", err, time.Since(started))
	}
}

func TestExplicitDoctorProbeIsQueryOnly(t *testing.T) {
	f := newStateFake(t)
	if err := f.client(t).Probe(context.Background(), "current"); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.mutations) != 0 {
		t.Fatalf("probe mutations=%v", f.mutations)
	}
	for _, operation := range f.transcript {
		if strings.Contains(operation, "Update") || strings.Contains(operation, "Create") {
			t.Fatalf("mutation in transcript: %v", f.transcript)
		}
	}
	t.Logf("query-only transcript=%v mutation-cardinality=%v", f.transcript, f.mutations)
}
