package server

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// LogInsightAction describes a remediation the panel can surface for a detected log issue.
type LogInsightAction struct {
	Kind    string            `json:"kind"`
	Label   string            `json:"label"`
	Payload map[string]string `json:"payload,omitempty"`
}

// LogInsight captures a structured interpretation of a console line along with suggested fixes.
type LogInsight struct {
	Kind       string             `json:"kind"`
	Severity   string             `json:"severity"`
	Summary    string             `json:"summary"`
	Detail     string             `json:"detail,omitempty"`
	Actions    []LogInsightAction `json:"actions,omitempty"`
	SourceLine string             `json:"source_line"`
}

type insightTracker struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newInsightTracker() *insightTracker {
	return &insightTracker{
		seen: make(map[string]time.Time),
	}
}

func (it *insightTracker) shouldPublish(key string) bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	now := time.Now()
	if ts, ok := it.seen[key]; ok && now.Sub(ts) < 2*time.Minute {
		return false
	}

	// prune old entries to avoid unbounded growth
	for k, ts := range it.seen {
		if now.Sub(ts) > 10*time.Minute {
			delete(it.seen, k)
		}
	}

	it.seen[key] = now
	return true
}

var (
	missingDependencyPattern = regexp.MustCompile(`(?i)missing required (?:mod|dependency):\s*([A-Za-z0-9._-]+)`)
	datapackFailurePattern   = regexp.MustCompile(`(?i)(?:failed to load|error(?:s)? (?:in|reloading)?)\s+(?:data ?pack|datapack)[^:]*[:"]\s*([^"']+)`)
	registryMismatchPattern  = regexp.MustCompile(`(?i)(mismatched mod channel list|registry remapping failed|incompatible mod set)`)
	duplicateModPattern      = regexp.MustCompile(`(?i)duplicate mods?:\s*([A-Za-z0-9._-]+)`)
)

// analyzeConsoleLine inspects a single console line for actionable Minecraft issues
// and emits structured insights for the panel to present.
func (s *Server) analyzeConsoleLine(data []byte) {
	line := stripAnsiRegex.ReplaceAll(data, []byte(""))
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return
	}

	text := string(line)

	for _, detector := range []func(string) *LogInsight{
		detectMissingDependency,
		detectDatapackFailure,
		detectRegistryMismatch,
		detectDuplicateMod,
	} {
		if insight := detector(text); insight != nil {
			insight.SourceLine = text
			s.publishInsight(*insight)
		}
	}
}

func (s *Server) publishInsight(insight LogInsight) {
	key := insight.cacheKey()
	if s.insights != nil && !s.insights.shouldPublish(key) {
		return
	}
	s.Events().Publish(ConsoleInsightEvent, insight)
}

func detectMissingDependency(line string) *LogInsight {
	matches := missingDependencyPattern.FindStringSubmatch(line)
	if len(matches) < 2 {
		return nil
	}

	mod := matches[1]
	return &LogInsight{
		Kind:     "missing_dependency",
		Severity: "warning",
		Summary:  fmt.Sprintf("Missing dependency: %s", mod),
		Detail:   "Minecraft reported a missing mod dependency. Add the required mod or remove the mod that depends on it.",
		Actions: []LogInsightAction{
			{
				Kind:  "download_dependency",
				Label: fmt.Sprintf("Download %s", mod),
				Payload: map[string]string{
					"mod_id": mod,
				},
			},
		},
	}
}

func detectDatapackFailure(line string) *LogInsight {
	matches := datapackFailurePattern.FindStringSubmatch(line)
	if len(matches) < 2 {
		return nil
	}

	pack := strings.TrimSpace(matches[1])
	return &LogInsight{
		Kind:     "datapack_failure",
		Severity: "warning",
		Summary:  fmt.Sprintf("Datapack failed to load: %s", pack),
		Detail:   "A datapack failed validation or crashed during load. Disabling the pack can allow the server to start cleanly.",
		Actions: []LogInsightAction{
			{
				Kind:  "disable_datapack",
				Label: fmt.Sprintf("Disable datapack \"%s\"", pack),
				Payload: map[string]string{
					"datapack": pack,
				},
			},
		},
	}
}

func detectRegistryMismatch(line string) *LogInsight {
	if !registryMismatchPattern.MatchString(line) {
		return nil
	}

	return &LogInsight{
		Kind:     "registry_mismatch",
		Severity: "warning",
		Summary:  "Registry or mod channel mismatch detected",
		Detail:   "The server reported incompatible registries or mod channel mismatches. Align client/server mod versions or roll back the last mod update.",
	}
}

func detectDuplicateMod(line string) *LogInsight {
	matches := duplicateModPattern.FindStringSubmatch(line)
	if len(matches) < 2 {
		return nil
	}

	mod := matches[1]
	return &LogInsight{
		Kind:     "duplicate_mod",
		Severity: "warning",
		Summary:  fmt.Sprintf("Duplicate mod detected: %s", mod),
		Detail:   "Duplicate mods can prevent the server from starting. Remove the extra copy and restart.",
	}
}

func (i LogInsight) cacheKey() string {
	return strings.ToLower(i.Kind + "|" + i.Summary)
}
