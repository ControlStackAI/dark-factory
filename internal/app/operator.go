package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ControlStackAI/dark-factory/internal/config"
	"github.com/ControlStackAI/dark-factory/internal/domain"
)

var ErrLiveAdaptersUnavailable = errors.New("live execution is not implemented until M2/M3; no external action was taken")

type Operator struct{ Config config.Config }

type Check struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type DoctorReport struct {
	Status string  `json:"status"`
	Checks []Check `json:"checks"`
}

type ValidationReport struct {
	ConfigVersion int    `json:"config_version"`
	Mode          string `json:"mode"`
	Status        string `json:"status"`
}

type StatusReport struct {
	Database string `json:"database"`
	Status   string `json:"status"`
	Message  string `json:"message"`
}

type RunReport struct {
	ID        string           `json:"id"`
	ProjectID string           `json:"project_id"`
	IssueID   string           `json:"issue_id"`
	Status    domain.RunStatus `json:"status"`
	Step      string           `json:"step"`
	Attempts  int              `json:"attempts"`
	Evidence  []string         `json:"evidence"`
}

func Compose(path string, requireSecret bool) (*Operator, error) {
	cfg, err := config.Load(path, config.LoadOptions{RequireSecret: requireSecret})
	if err != nil {
		return nil, err
	}
	return &Operator{Config: cfg}, nil
}

func (o *Operator) Validation() ValidationReport {
	return ValidationReport{ConfigVersion: o.Config.ConfigVersion, Mode: o.Config.Mode, Status: "ready"}
}

func (o *Operator) DryRun(ctx context.Context) (domain.Run, error) { return DryRun(ctx) }

func SummarizeRun(run domain.Run) RunReport {
	return RunReport{ID: run.ID, ProjectID: run.ProjectID, IssueID: run.IssueID, Status: run.Status, Step: run.Step, Attempts: run.Attempts, Evidence: append([]string(nil), run.Evidence...)}
}

func (o *Operator) Once(ctx context.Context, apply bool) (domain.Run, error) {
	if o.Config.Mode != "live" {
		if apply {
			return domain.Run{}, errors.New("--apply requires config mode live; no external action was taken")
		}
		return o.DryRun(ctx)
	}
	if !apply {
		return domain.Run{}, errors.New("config mode live requires --apply; no external action was taken")
	}
	return domain.Run{}, ErrLiveAdaptersUnavailable
}

func (o *Operator) Doctor() DoctorReport {
	checks := []Check{{Name: "config", Status: "ready", Message: "schema and path policy validated"}}
	checks = append(checks, checkStateDB(o.Config.Paths.StateDB))
	_, secretErr := config.ResolveSecret(o.Config.Linear.APIKey)
	if secretErr != nil {
		checks = append(checks, Check{Name: "linear", Status: "not-ready", Message: "secret environment reference is unavailable; adapter is not implemented until M2"})
	} else {
		checks = append(checks, Check{Name: "linear", Status: "degraded", Message: "configuration is present; adapter is not implemented until M2"})
	}
	if executableAvailable(o.Config.OpenClaw.Executable) {
		checks = append(checks, Check{Name: "openclaw", Status: "degraded", Message: "executable is discoverable; executor is not implemented until M3"})
	} else {
		checks = append(checks, Check{Name: "openclaw", Status: "not-ready", Message: "executable is unavailable; executor is not implemented until M3"})
	}
	checks = append(checks, checkRoot("review-root", o.Config.Paths.ReviewRoot), checkRoot("artifact-root", o.Config.Paths.ArtifactRoot))
	if secretErr != nil {
		checks = append(checks, Check{Name: "service-environment", Status: "not-ready", Message: "required secret environment variable is not set"})
	} else {
		checks = append(checks, Check{Name: "service-environment", Status: "ready", Message: "required environment references are available"})
	}
	report := DoctorReport{Status: "ready", Checks: checks}
	for _, check := range checks {
		if check.Status == "not-ready" {
			report.Status = "not-ready"
			break
		}
		if check.Status == "degraded" {
			report.Status = "degraded"
		}
	}
	return report
}

func (o *Operator) Status() StatusReport {
	info, err := os.Lstat(o.Config.Paths.StateDB)
	if errors.Is(err, os.ErrNotExist) {
		return StatusReport{Database: o.Config.Paths.StateDB, Status: "not-started", Message: "state database does not exist"}
	}
	if err != nil {
		return StatusReport{Database: o.Config.Paths.StateDB, Status: "not-ready", Message: "state database cannot be inspected"}
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return StatusReport{Database: o.Config.Paths.StateDB, Status: "not-ready", Message: "state database is not a private regular file"}
	}
	return StatusReport{Database: o.Config.Paths.StateDB, Status: "present", Message: "state database exists; M1 status does not mutate or open it"}
}

func checkStateDB(path string) Check {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Check{Name: "state-db", Status: "degraded", Message: "database has not been created"}
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return Check{Name: "state-db", Status: "not-ready", Message: "database is not a private regular file"}
	}
	return Check{Name: "state-db", Status: "ready", Message: "private database file is present"}
}

func checkRoot(name, path string) Check {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Check{Name: name, Status: "not-ready", Message: "directory does not exist"}
	}
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return Check{Name: name, Status: "not-ready", Message: "directory is unavailable or not private"}
	}
	return Check{Name: name, Status: "ready", Message: "private directory is present"}
}

func executableAvailable(name string) bool {
	if strings.ContainsRune(name, filepath.Separator) {
		info, err := os.Stat(name)
		return err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0
	}
	_, err := exec.LookPath(name)
	return err == nil
}

func FormatDoctor(report DoctorReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "doctor: %s\n", report.Status)
	for _, check := range report.Checks {
		fmt.Fprintf(&b, "%-20s %-10s %s\n", check.Name, check.Status, check.Message)
	}
	return b.String()
}
