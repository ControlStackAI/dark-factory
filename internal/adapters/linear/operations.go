package linear

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

const (
	issueQuery      = `query DarkFactoryIssue($id:String!){issue(id:$id){id identifier title priority createdAt project{id} team{id} state{id name type}}}`
	statesQuery     = `query DarkFactoryStates($teamId:String!,$first:Int!,$after:String){team(id:$teamId){id states(first:$first,after:$after){nodes{id name type position} pageInfo{hasNextPage endCursor}}}}`
	issuesQuery     = `query DarkFactoryIssues($projectId:String!,$first:Int!,$after:String){project(id:$projectId){id issues(first:$first,after:$after){nodes{id identifier title priority createdAt project{id} team{id} state{id name type}} pageInfo{hasNextPage endCursor}}}}`
	relationsQuery  = `query DarkFactoryRelations($id:String!,$first:Int!,$after:String){issue(id:$id){id identifier project{id} team{id} relations(first:$first,after:$after){nodes{type relatedIssue{id identifier state{id name type}}} pageInfo{hasNextPage endCursor}}}}`
	commentsQuery   = `query DarkFactoryComments($id:String!,$first:Int!,$after:String){issue(id:$id){id identifier project{id} team{id} comments(first:$first,after:$after){nodes{id body} pageInfo{hasNextPage endCursor}}}}`
	updateMutation  = `mutation DarkFactoryUpdateIssue($id:String!,$input:IssueUpdateInput!){issueUpdate(id:$id,input:$input){success issue{id identifier title priority createdAt project{id} team{id} state{id name type}}}}`
	commentMutation = `mutation DarkFactoryCreateComment($input:CommentCreateInput!){commentCreate(input:$input){success comment{id}}}`
)

type pageInfo struct {
	HasNextPage *bool   `json:"hasNextPage"`
	EndCursor   *string `json:"endCursor"`
}

func (p pageInfo) next(connection string) (string, bool, error) {
	if p.HasNextPage == nil {
		return "", false, fmt.Errorf("Linear %s pagination omitted hasNextPage", connection)
	}
	if !*p.HasNextPage {
		return "", false, nil
	}
	if p.EndCursor == nil || *p.EndCursor == "" {
		return "", false, fmt.Errorf("Linear %s pagination returned an empty cursor", connection)
	}
	return *p.EndCursor, true, nil
}

type remoteState struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Position float64 `json:"position"`
}

type remoteIssue struct {
	ID         string `json:"id"`
	Identifier string `json:"identifier"`
	Title      string `json:"title"`
	Priority   int    `json:"priority"`
	CreatedAt  string `json:"createdAt"`
	Project    *struct {
		ID string `json:"id"`
	} `json:"project"`
	Team struct {
		ID string `json:"id"`
	} `json:"team"`
	State remoteState `json:"state"`
}

type lifecycle struct {
	ready      remoteState
	inProgress remoteState
	done       remoteState
}

func (c *Client) resolveLifecycle(ctx context.Context) (lifecycle, error) {
	var states []remoteState
	var cursor any
	for page := 0; page < c.maxPages; page++ {
		var data struct {
			Team *struct {
				ID     string `json:"id"`
				States struct {
					Nodes    []remoteState `json:"nodes"`
					PageInfo pageInfo      `json:"pageInfo"`
				} `json:"states"`
			} `json:"team"`
		}
		err := c.query(ctx, "DarkFactoryStates", statesQuery, map[string]any{"teamId": c.teamID, "first": c.pageSize, "after": cursor}, &data)
		if err != nil {
			return lifecycle{}, err
		}
		if data.Team == nil || data.Team.ID != c.teamID {
			return lifecycle{}, fmt.Errorf("%w: configured Linear team was not returned exactly", ports.ErrNotFound)
		}
		states = append(states, data.Team.States.Nodes...)
		nextCursor, more, pageErr := data.Team.States.PageInfo.next("lifecycle")
		if pageErr != nil {
			return lifecycle{}, pageErr
		}
		if !more {
			ready, err := chooseState(states, "unstarted", c.readyName)
			if err != nil {
				return lifecycle{}, fmt.Errorf("Ready lifecycle state: %w", err)
			}
			started, err := chooseState(states, "started", c.inProgressName)
			if err != nil {
				return lifecycle{}, fmt.Errorf("In Progress lifecycle state: %w", err)
			}
			done, err := chooseState(states, "completed", c.doneName)
			if err != nil {
				return lifecycle{}, fmt.Errorf("Done lifecycle state: %w", err)
			}
			return lifecycle{ready: ready, inProgress: started, done: done}, nil
		}
		cursor = nextCursor
	}
	return lifecycle{}, fmt.Errorf("Linear lifecycle pagination exceeded %d pages", c.maxPages)
}

func chooseState(states []remoteState, stateType, preferred string) (remoteState, error) {
	var candidates []remoteState
	for _, state := range states {
		if state.Type == stateType {
			if state.ID == "" || state.Name == "" {
				return remoteState{}, fmt.Errorf("incomplete state with lifecycle type %q", stateType)
			}
			candidates = append(candidates, state)
		}
	}
	if len(candidates) == 0 {
		return remoteState{}, fmt.Errorf("no state with lifecycle type %q", stateType)
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	var preferredMatches []remoteState
	for _, state := range candidates {
		if state.Name == preferred {
			preferredMatches = append(preferredMatches, state)
		}
	}
	if len(preferredMatches) == 1 {
		return preferredMatches[0], nil
	}
	return remoteState{}, fmt.Errorf("ambiguous lifecycle type %q; preferred name %q did not identify exactly one state", stateType, preferred)
}

func (c *Client) getRemoteIssue(ctx context.Context, id string) (remoteIssue, error) {
	if !c.allowedReference(id) {
		return remoteIssue{}, fmt.Errorf("%w: requested issue is outside the configured allowlist", ports.ErrConflict)
	}
	var data struct {
		Issue *remoteIssue `json:"issue"`
	}
	if err := c.query(ctx, "DarkFactoryIssue", issueQuery, map[string]any{"id": id}, &data); err != nil {
		return remoteIssue{}, err
	}
	if data.Issue == nil {
		return remoteIssue{}, ports.ErrNotFound
	}
	if data.Issue.ID != id && data.Issue.Identifier != id {
		return remoteIssue{}, fmt.Errorf("%w: Linear issue query returned a different issue than requested", ports.ErrConflict)
	}
	if err := c.enforceScope(*data.Issue); err != nil {
		return remoteIssue{}, err
	}
	return *data.Issue, nil
}

func (c *Client) GetIssue(ctx context.Context, id string) (domain.Issue, error) {
	issue, err := c.getRemoteIssue(ctx, id)
	if err != nil {
		return domain.Issue{}, err
	}
	return toDomainIssue(issue, false)
}

func (c *Client) listRemoteIssues(ctx context.Context) ([]remoteIssue, error) {
	var issues []remoteIssue
	seenIDs := make(map[string]struct{})
	seenIdentifiers := make(map[string]struct{})
	var cursor any
	for page := 0; page < c.maxPages; page++ {
		var data struct {
			Project *struct {
				ID     string `json:"id"`
				Issues struct {
					Nodes    []remoteIssue `json:"nodes"`
					PageInfo pageInfo      `json:"pageInfo"`
				} `json:"issues"`
			} `json:"project"`
		}
		if err := c.query(ctx, "DarkFactoryIssues", issuesQuery, map[string]any{"projectId": c.projectID, "first": c.pageSize, "after": cursor}, &data); err != nil {
			return nil, err
		}
		if data.Project == nil || data.Project.ID != c.projectID {
			return nil, fmt.Errorf("%w: configured Linear project was not returned exactly", ports.ErrNotFound)
		}
		for _, issue := range data.Project.Issues.Nodes {
			if err := c.enforceTeamProject(issue); err != nil {
				return nil, fmt.Errorf("Linear project query scope validation failed: %w", err)
			}
			if err := validateRemoteIssue(issue); err != nil {
				return nil, err
			}
			if _, duplicate := seenIDs[issue.ID]; duplicate {
				return nil, fmt.Errorf("%w: Linear issue pagination returned a duplicate issue ID", ports.ErrConflict)
			}
			if _, duplicate := seenIdentifiers[issue.Identifier]; duplicate {
				return nil, fmt.Errorf("%w: Linear issue pagination returned a duplicate identifier", ports.ErrConflict)
			}
			seenIDs[issue.ID] = struct{}{}
			seenIdentifiers[issue.Identifier] = struct{}{}
			if c.allowedIssue(issue) {
				issues = append(issues, issue)
			}
		}
		nextCursor, more, pageErr := data.Project.Issues.PageInfo.next("issue")
		if pageErr != nil {
			return nil, pageErr
		}
		if !more {
			return issues, nil
		}
		cursor = nextCursor
	}
	return nil, fmt.Errorf("Linear issue pagination exceeded %d pages", c.maxPages)
}

func (c *Client) ListProjectIssues(ctx context.Context, projectID string) ([]domain.Issue, error) {
	if projectID != c.projectID {
		return nil, fmt.Errorf("%w: project does not match configured Linear project", ports.ErrConflict)
	}
	remote, err := c.listRemoteIssues(ctx)
	if err != nil {
		return nil, err
	}
	issues := make([]domain.Issue, 0, len(remote))
	for _, issue := range remote {
		if issue.State.Type != "unstarted" && issue.State.Type != "started" && issue.State.Type != "completed" {
			continue
		}
		mapped, err := toDomainIssue(issue, false)
		if err != nil {
			return nil, err
		}
		issues = append(issues, mapped)
	}
	return issues, nil
}

func toDomainIssue(issue remoteIssue, blocked bool) (domain.Issue, error) {
	if err := validateRemoteIssue(issue); err != nil {
		return domain.Issue{}, err
	}
	created, err := time.Parse(time.RFC3339Nano, issue.CreatedAt)
	if err != nil {
		return domain.Issue{}, errors.New("Linear returned an invalid issue timestamp")
	}
	var state domain.IssueState
	switch issue.State.Type {
	case "unstarted":
		state = domain.IssueReady
	case "started":
		state = domain.IssueInProgress
	case "completed":
		state = domain.IssueCompleted
	default:
		return domain.Issue{}, fmt.Errorf("Linear issue %q has unsupported lifecycle type", issue.Identifier)
	}
	return domain.Issue{ID: issue.ID, Identifier: issue.Identifier, ProjectID: issue.Project.ID,
		Title: issue.Title, Priority: issue.Priority, CreatedAt: created, State: state, Blocked: blocked}, nil
}

func validateRemoteIssue(issue remoteIssue) error {
	if issue.Priority < 0 || issue.Priority > 4 {
		return errors.New("Linear returned an invalid issue priority")
	}
	if _, err := time.Parse(time.RFC3339Nano, issue.CreatedAt); err != nil {
		return errors.New("Linear returned an invalid issue timestamp")
	}
	if issue.State.Name == "" {
		return errors.New("Linear returned an incomplete issue state")
	}
	switch issue.State.Type {
	case "backlog", "unstarted", "started", "completed", "canceled":
		return nil
	default:
		return errors.New("Linear returned an unknown lifecycle type")
	}
}

func (c *Client) issueBlocked(ctx context.Context, issue remoteIssue) (bool, error) {
	var cursor any
	for page := 0; page < c.maxPages; page++ {
		var data struct {
			Issue *struct {
				ID         string `json:"id"`
				Identifier string `json:"identifier"`
				Project    *struct {
					ID string `json:"id"`
				} `json:"project"`
				Team struct {
					ID string `json:"id"`
				} `json:"team"`
				Relations struct {
					Nodes []struct {
						Type         string `json:"type"`
						RelatedIssue struct {
							ID         string      `json:"id"`
							Identifier string      `json:"identifier"`
							State      remoteState `json:"state"`
						} `json:"relatedIssue"`
					} `json:"nodes"`
					PageInfo pageInfo `json:"pageInfo"`
				} `json:"relations"`
			} `json:"issue"`
		}
		if err := c.query(ctx, "DarkFactoryRelations", relationsQuery, map[string]any{"id": issue.ID, "first": c.pageSize, "after": cursor}, &data); err != nil {
			return false, err
		}
		if data.Issue == nil || data.Issue.ID != issue.ID || data.Issue.Project == nil ||
			data.Issue.Project.ID != c.projectID || data.Issue.Team.ID != c.teamID {
			return false, fmt.Errorf("%w: Linear relation query returned wrong issue scope", ports.ErrConflict)
		}
		for _, relation := range data.Issue.Relations.Nodes {
			typeName := strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(relation.Type))
			if typeName == "blockedby" && relation.RelatedIssue.State.Type != "completed" && relation.RelatedIssue.State.Type != "canceled" {
				return true, nil
			}
		}
		nextCursor, more, pageErr := data.Issue.Relations.PageInfo.next("relation")
		if pageErr != nil {
			return false, pageErr
		}
		if !more {
			return false, nil
		}
		cursor = nextCursor
	}
	return false, fmt.Errorf("Linear relation pagination exceeded %d pages", c.maxPages)
}

// SelectReady completely paginates project issues and blockers before choosing. The current
// issue is excluded by both UUID and human identifier.
func (c *Client) SelectReady(ctx context.Context, currentID string) (domain.Issue, bool, error) {
	if currentID != "" && !c.allowedReference(currentID) {
		return domain.Issue{}, false, fmt.Errorf("%w: current issue is outside the configured allowlist", ports.ErrConflict)
	}
	lifecycle, err := c.resolveLifecycle(ctx)
	if err != nil {
		return domain.Issue{}, false, err
	}
	issues, err := c.listRemoteIssues(ctx)
	if err != nil {
		return domain.Issue{}, false, err
	}
	var candidates []domain.Issue
	for _, issue := range issues {
		if issue.ID == currentID || issue.Identifier == currentID || issue.State.ID != lifecycle.ready.ID {
			continue
		}
		blocked, err := c.issueBlocked(ctx, issue)
		if err != nil {
			return domain.Issue{}, false, err
		}
		mapped, err := toDomainIssue(issue, blocked)
		if err != nil {
			return domain.Issue{}, false, err
		}
		candidates = append(candidates, mapped)
	}
	selected, found := domain.SelectNextExcluding(candidates, currentID)
	return selected, found, nil
}

type ClaimRequest struct {
	RunID          string
	IssueID        string
	IdempotencyKey string
}

// Claim acts only on an already-frozen exact issue/key and never selects a replacement.
// The controller must persist that intent between SelectReady and Claim and before agent work.
func (c *Client) Claim(ctx context.Context, request ClaimRequest) error {
	if request.RunID == "" || request.IssueID == "" || request.IdempotencyKey == "" {
		return errors.New("incomplete frozen Linear claim")
	}
	if !c.allowedReference(request.IssueID) {
		return fmt.Errorf("%w: frozen claim is outside the configured allowlist", ports.ErrConflict)
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	states, err := c.resolveLifecycle(ctx)
	if err != nil {
		return err
	}
	issue, err := c.getRemoteIssue(ctx, request.IssueID)
	if err != nil {
		return err
	}
	claimIntent := commentIntent{
		Version: 1, Key: request.IdempotencyKey, RunID: request.RunID, ProjectID: c.projectID,
		IssueID: issue.ID, Transition: "started",
		Evidence: fmt.Sprintf("%x", sha256.Sum256([]byte("claim:"+request.RunID+":"+issue.ID))),
	}
	if issue.State.ID == states.inProgress.ID {
		bodies, listErr := c.listComments(ctx, issue)
		if listErr != nil {
			return listErr
		}
		exists, inspectErr := inspectComments(bodies, claimIntent)
		if inspectErr != nil {
			return inspectErr
		}
		if !exists {
			return fmt.Errorf("%w: issue is already In Progress without the matching frozen claim", ports.ErrConflict)
		}
		return nil
	}
	if issue.State.ID != states.ready.ID {
		return fmt.Errorf("%w: frozen claim issue is not in the resolved Ready state", ports.ErrInvalidTransition)
	}
	blocked, err := c.issueBlocked(ctx, issue)
	if err != nil {
		return err
	}
	if blocked {
		return fmt.Errorf("%w: frozen claim issue became blocked before adoption", ports.ErrInvalidTransition)
	}
	if err := c.ensureComment(ctx, issue, claimIntent, "Dark Factory claimed this frozen Ready issue before agent execution."); err != nil {
		return err
	}
	issue, err = c.getRemoteIssue(ctx, issue.ID)
	if err != nil {
		return err
	}
	return c.updateStateReconciled(ctx, issue, states.inProgress)
}

func (c *Client) updateStateReconciled(ctx context.Context, issue remoteIssue, desired remoteState) error {
	var data struct {
		IssueUpdate struct {
			Success bool        `json:"success"`
			Issue   remoteIssue `json:"issue"`
		} `json:"issueUpdate"`
	}
	err := c.mutate(ctx, "DarkFactoryUpdateIssue", updateMutation, map[string]any{"id": issue.ID, "input": map[string]any{"stateId": desired.ID}}, &data)
	if err == nil {
		if !data.IssueUpdate.Success {
			return errors.New("Linear issue state mutation reported failure")
		}
		if scopeErr := c.enforceScope(data.IssueUpdate.Issue); scopeErr != nil {
			return scopeErr
		}
		if data.IssueUpdate.Issue.State.ID != desired.ID {
			return errors.New("Linear issue state mutation returned an unexpected state")
		}
	}
	verified, verifyErr := c.getRemoteIssue(ctx, issue.ID)
	if verifyErr == nil && verified.State.ID == desired.ID {
		return nil
	}
	if err != nil {
		return err
	}
	if verifyErr != nil {
		return fmt.Errorf("Linear state verification failed after mutation: %w", verifyErr)
	}
	return errors.New("Linear state verification did not observe the intended state")
}

type commentIntent struct {
	Version    int    `json:"version"`
	Key        string `json:"key"`
	RunID      string `json:"run_id"`
	ProjectID  string `json:"project_id"`
	IssueID    string `json:"issue_id"`
	Transition string `json:"transition"`
	ReviewID   string `json:"review_id"`
	Evidence   string `json:"evidence_sha256"`
}

const markerPrefix = "<!-- dark-factory:"

func marker(intent commentIntent) string {
	b, _ := json.Marshal(intent)
	return markerPrefix + string(b) + " -->"
}

func parseMarker(body string) (commentIntent, bool) {
	first, _, _ := strings.Cut(body, "\n")
	if !strings.HasPrefix(first, markerPrefix) || !strings.HasSuffix(first, " -->") {
		return commentIntent{}, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(first, markerPrefix), " -->")
	var intent commentIntent
	if json.Unmarshal([]byte(raw), &intent) != nil || intent.Version != 1 || intent.Key == "" {
		return commentIntent{}, false
	}
	return intent, true
}

func (c *Client) listComments(ctx context.Context, issue remoteIssue) ([]string, error) {
	var bodies []string
	var cursor any
	for page := 0; page < c.maxPages; page++ {
		var data struct {
			Issue *struct {
				ID         string `json:"id"`
				Identifier string `json:"identifier"`
				Project    *struct {
					ID string `json:"id"`
				} `json:"project"`
				Team struct {
					ID string `json:"id"`
				} `json:"team"`
				Comments struct {
					Nodes    []struct{ ID, Body string } `json:"nodes"`
					PageInfo pageInfo                    `json:"pageInfo"`
				} `json:"comments"`
			} `json:"issue"`
		}
		if err := c.query(ctx, "DarkFactoryComments", commentsQuery, map[string]any{"id": issue.ID, "first": c.pageSize, "after": cursor}, &data); err != nil {
			return nil, err
		}
		if data.Issue == nil || data.Issue.ID != issue.ID || data.Issue.Project == nil || data.Issue.Project.ID != c.projectID || data.Issue.Team.ID != c.teamID {
			return nil, fmt.Errorf("%w: Linear comment query returned wrong issue scope", ports.ErrConflict)
		}
		for _, comment := range data.Issue.Comments.Nodes {
			bodies = append(bodies, comment.Body)
		}
		nextCursor, more, pageErr := data.Issue.Comments.PageInfo.next("comment")
		if pageErr != nil {
			return nil, pageErr
		}
		if !more {
			return bodies, nil
		}
		cursor = nextCursor
	}
	return nil, fmt.Errorf("Linear comment pagination exceeded %d pages", c.maxPages)
}

func inspectComments(bodies []string, intended commentIntent) (bool, error) {
	exact := 0
	for _, body := range bodies {
		found, ok := parseMarker(body)
		if !ok {
			first, _, _ := strings.Cut(body, "\n")
			if strings.HasPrefix(first, markerPrefix) {
				return false, fmt.Errorf("%w: malformed controller comment marker", ports.ErrConflict)
			}
			continue
		}
		if found == intended {
			exact++
			continue
		}
		if found.Key == intended.Key || (found.ProjectID == intended.ProjectID && found.IssueID == intended.IssueID && found.Transition == intended.Transition) {
			return false, fmt.Errorf("%w: conflicting controller comment idempotency key or intent", ports.ErrConflict)
		}
	}
	if exact > 1 {
		return false, fmt.Errorf("%w: duplicate controller comments already exist", ports.ErrConflict)
	}
	return exact == 1, nil
}

func (c *Client) ensureComment(ctx context.Context, issue remoteIssue, intended commentIntent, text string) error {
	bodies, err := c.listComments(ctx, issue)
	if err != nil {
		return err
	}
	exists, err := inspectComments(bodies, intended)
	if err != nil || exists {
		return err
	}
	body := marker(intended) + "\n" + text
	var data struct {
		CommentCreate struct {
			Success bool `json:"success"`
			Comment struct {
				ID string `json:"id"`
			} `json:"comment"`
		} `json:"commentCreate"`
	}
	mutationErr := c.mutate(ctx, "DarkFactoryCreateComment", commentMutation, map[string]any{"input": map[string]any{"issueId": issue.ID, "body": body}}, &data)
	if mutationErr == nil && (!data.CommentCreate.Success || data.CommentCreate.Comment.ID == "") {
		mutationErr = errors.New("Linear comment mutation reported failure")
	}
	// Every create, including an apparently successful one, is verified by a fresh query.
	bodies, verifyErr := c.listComments(ctx, issue)
	if verifyErr == nil {
		exists, inspectErr := inspectComments(bodies, intended)
		if inspectErr != nil {
			return inspectErr
		}
		if exists {
			return nil
		}
	}
	if mutationErr != nil {
		return mutationErr
	}
	if verifyErr != nil {
		return fmt.Errorf("Linear comment verification failed after mutation: %w", verifyErr)
	}
	return errors.New("Linear comment verification did not observe the intended comment")
}

func (c *Client) Advance(ctx context.Context, request domain.AdvanceRequest) error {
	if request.RunID == "" || request.ProjectID != c.projectID || request.CurrentIssueID == "" ||
		request.Evidence == "" || request.ReviewID == "" || request.IdempotencyKey == "" ||
		request.Fence == 0 || request.CurrentIssueID == request.NextIssueID {
		return errors.New("incomplete or out-of-scope frozen Linear advancement")
	}
	if !c.allowedReference(request.CurrentIssueID) || (request.NextIssueID != "" && !c.allowedReference(request.NextIssueID)) {
		return fmt.Errorf("%w: frozen advancement is outside the configured allowlist", ports.ErrConflict)
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	states, err := c.resolveLifecycle(ctx)
	if err != nil {
		return err
	}
	current, err := c.getRemoteIssue(ctx, request.CurrentIssueID)
	if err != nil {
		return err
	}
	var next remoteIssue
	if request.NextIssueID != "" {
		next, err = c.getRemoteIssue(ctx, request.NextIssueID)
		if err != nil {
			return err
		}
		if next.State.ID == states.ready.ID {
			blocked, blockErr := c.issueBlocked(ctx, next)
			if blockErr != nil {
				return blockErr
			}
			if blocked {
				return fmt.Errorf("%w: frozen next issue became blocked before adoption", ports.ErrInvalidTransition)
			}
		}
	}
	evidenceDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(request.Evidence)))
	completeIntent := commentIntent{Version: 1, Key: request.IdempotencyKey, RunID: request.RunID, ProjectID: c.projectID, IssueID: current.ID, Transition: "completed", ReviewID: request.ReviewID, Evidence: evidenceDigest}
	if current.State.ID == states.done.ID {
		bodies, listErr := c.listComments(ctx, current)
		if listErr != nil {
			return listErr
		}
		exists, inspectErr := inspectComments(bodies, completeIntent)
		if inspectErr != nil {
			return inspectErr
		}
		if !exists {
			return fmt.Errorf("%w: current issue is already Done without the matching frozen controller intent", ports.ErrConflict)
		}
	} else {
		if current.State.ID != states.inProgress.ID {
			return fmt.Errorf("%w: current issue is neither resolved In Progress nor Done", ports.ErrInvalidTransition)
		}
		if err := c.ensureComment(ctx, current, completeIntent, "Dark Factory completion evidence digest: sha256:"+evidenceDigest+" (review "+request.ReviewID+")"); err != nil {
			return err
		}
		current, err = c.getRemoteIssue(ctx, current.ID)
		if err != nil {
			return err
		}
		if err := c.updateStateReconciled(ctx, current, states.done); err != nil {
			return err
		}
	}
	if request.NextIssueID != "" {
		adoptIntent := commentIntent{Version: 1, Key: request.IdempotencyKey, RunID: request.RunID, ProjectID: c.projectID, IssueID: next.ID, Transition: "started", ReviewID: request.ReviewID, Evidence: evidenceDigest}
		if next.State.ID == states.inProgress.ID {
			bodies, listErr := c.listComments(ctx, next)
			if listErr != nil {
				return listErr
			}
			exists, inspectErr := inspectComments(bodies, adoptIntent)
			if inspectErr != nil {
				return inspectErr
			}
			if !exists {
				return fmt.Errorf("%w: next issue is already In Progress without the matching frozen controller intent", ports.ErrConflict)
			}
		} else {
			if next.State.ID != states.ready.ID {
				return fmt.Errorf("%w: frozen next issue is neither resolved Ready nor In Progress", ports.ErrInvalidTransition)
			}
			if err := c.ensureComment(ctx, next, adoptIntent, "Dark Factory adopted this frozen next issue after "+current.Identifier+"."); err != nil {
				return err
			}
			next, err = c.getRemoteIssue(ctx, next.ID)
			if err != nil {
				return err
			}
			if err := c.updateStateReconciled(ctx, next, states.inProgress); err != nil {
				return err
			}
		}
	}
	return c.verifyAdvance(ctx, request, states, completeIntent)
}

func (c *Client) verifyAdvance(ctx context.Context, request domain.AdvanceRequest, states lifecycle, completeIntent commentIntent) error {
	current, err := c.getRemoteIssue(ctx, request.CurrentIssueID)
	if err != nil || current.State.ID != states.done.ID {
		return errors.New("final Linear verification did not observe current issue Done")
	}
	bodies, err := c.listComments(ctx, current)
	if err != nil {
		return err
	}
	if exists, inspectErr := inspectComments(bodies, completeIntent); inspectErr != nil || !exists {
		if inspectErr != nil {
			return inspectErr
		}
		return errors.New("final Linear verification did not observe completion comment")
	}
	if request.NextIssueID == "" {
		return nil
	}
	next, err := c.getRemoteIssue(ctx, request.NextIssueID)
	if err != nil || next.State.ID != states.inProgress.ID {
		return errors.New("final Linear verification did not observe frozen next issue In Progress")
	}
	intent := commentIntent{Version: 1, Key: request.IdempotencyKey, RunID: request.RunID, ProjectID: c.projectID, IssueID: next.ID, Transition: "started", ReviewID: request.ReviewID, Evidence: completeIntent.Evidence}
	bodies, err = c.listComments(ctx, next)
	if err != nil {
		return err
	}
	if exists, inspectErr := inspectComments(bodies, intent); inspectErr != nil || !exists {
		if inspectErr != nil {
			return inspectErr
		}
		return errors.New("final Linear verification did not observe adoption comment")
	}
	return nil
}

// Probe is an explicit online, query-only readiness check. It validates exact scope and all
// lifecycle states; it never invokes a mutation operation.
func (c *Client) Probe(ctx context.Context, issueID string) error {
	if _, err := c.resolveLifecycle(ctx); err != nil {
		return err
	}
	_, err := c.getRemoteIssue(ctx, issueID)
	return err
}
