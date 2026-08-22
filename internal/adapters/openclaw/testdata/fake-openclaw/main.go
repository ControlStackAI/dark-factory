package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type transcript struct {
	Executable          string   `json:"executable"`
	Args                []string `json:"args"`
	Prompt              string   `json:"prompt"`
	PromptMode          uint32   `json:"prompt_mode"`
	PID                 int      `json:"pid"`
	LinearSecretPresent bool     `json:"linear_secret_present"`
}

func main() {
	messageFile := valueAfter("--message-file")
	prompt, err := os.ReadFile(messageFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "prompt read failed")
		os.Exit(90)
	}
	info, _ := os.Stat(messageFile)
	_, linearPresent := os.LookupEnv("LINEAR_API_KEY")
	record := transcript{Executable: os.Args[0], Args: os.Args[1:], Prompt: string(prompt), PromptMode: uint32(info.Mode().Perm()), PID: os.Getpid(), LinearSecretPresent: linearPresent}
	if path := os.Getenv("FAKE_OPENCLAW_TRANSCRIPT"); path != "" {
		encoded, _ := json.Marshal(record)
		_ = os.WriteFile(path, append(encoded, '\n'), 0o600)
	}
	scenario := os.Getenv("FAKE_OPENCLAW_SCENARIO")
	switch scenario {
	case "invalid-json":
		fmt.Print("{not-json")
	case "oversized-stdout":
		fmt.Print(strings.Repeat("x", intEnv("FAKE_OPENCLAW_BYTES", 65536)))
	case "oversized-stderr":
		fmt.Fprint(os.Stderr, strings.Repeat("secret=should-not-leak ", intEnv("FAKE_OPENCLAW_BYTES", 65536)))
		success()
	case "nonzero":
		fmt.Fprintln(os.Stderr, "Authorization: bearer-value api_key=secret-value token=hunter2")
		firstLine, _, _ := strings.Cut(string(prompt), "\n")
		fmt.Fprintln(os.Stderr, "prompt prefix:", firstLine)
		fmt.Fprintln(os.Stderr, string(prompt))
		os.Exit(42)
	case "delayed-timeout":
		time.Sleep(durationEnv("FAKE_OPENCLAW_DELAY", 10*time.Second))
		success()
	case "signal-death":
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	case "child-timeout":
		child := exec.Command("sleep", "60")
		_ = child.Start()
		if path := os.Getenv("FAKE_OPENCLAW_CHILD_PID"); path != "" {
			_ = os.WriteFile(path, []byte(strconv.Itoa(child.Process.Pid)), 0o600)
		}
		time.Sleep(60 * time.Second)
	case "deadline-success":
		time.Sleep(durationEnv("FAKE_OPENCLAW_DELAY", 5*time.Millisecond))
		success()
	default:
		success()
	}
}

func success() {
	inner, _ := json.Marshal(map[string]any{"result_version": 1, "step": "fake OpenClaw completed", "evidence": "fake executable result artifact"})
	outer := map[string]any{"runId": "fake", "status": "ok", "summary": "completed", "result": map[string]any{"payloads": []map[string]any{{"text": string(inner)}}}}
	_ = json.NewEncoder(os.Stdout).Encode(outer)
}

func valueAfter(flag string) string {
	for index := 1; index+1 < len(os.Args); index++ {
		if os.Args[index] == flag {
			return os.Args[index+1]
		}
	}
	return ""
}

func intEnv(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return fallback
	}
	return value
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(name))
	if err != nil {
		return fallback
	}
	return value
}
