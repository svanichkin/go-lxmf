package main

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/svanichkin/configobj"
)

func TestParseCommaList(t *testing.T) {
	out := parseCommaList("foo,  bar , ,baz")
	if len(out) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(out))
	}
	if out[1] != "bar" {
		t.Fatalf("expected trimmed bar, got %q", out[1])
	}
}

func TestLoadHashList(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "ignored")
	content := "abcd\n\ntweet\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write ignored: %v", err)
	}
	list := loadHashList(path)
	if len(list) != 1 {
		t.Fatalf("expected 1 decoded hash, got %d", len(list))
	}
	if hex.EncodeToString(list[0]) != "abcd" {
		t.Fatalf("unexpected hash: %x", list[0])
	}
}

func TestApplyConfigReadsSections(t *testing.T) {
	origConfig := lxmdConfig
	origActive := activeConfig
	origIgnored := ignoredPath
	origAllowed := allowedPath
	defer func() {
		lxmdConfig = origConfig
		activeConfig = origActive
		ignoredPath = origIgnored
		allowedPath = origAllowed
	}()

	confStr := `
[lxmf]
display_name = testname
announce_at_start = yes
announce_interval = 5
delivery_transfer_max_accepted_size = 50

[propagation]
enable_node = yes
propagation_stamp_cost_target = 18
static_peers = aabb
control_allowed = bbaa, ccdd
`
	cfg, err := configobj.LoadReader(strings.NewReader(confStr))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	lxmdConfig = cfg
	tmpDir := t.TempDir()
	ignoredPath = filepath.Join(tmpDir, "ignored")
	allowedPath = filepath.Join(tmpDir, "allowed")
	os.WriteFile(ignoredPath, []byte("aabb\n"), 0o600)
	os.WriteFile(allowedPath, []byte("ccdd\n"), 0o600)

	if err := applyConfig(); err != nil {
		t.Fatalf("apply config: %v", err)
	}
	if activeConfig.DisplayName != "testname" {
		t.Fatalf("expected display_name testname, got %q", activeConfig.DisplayName)
	}
	if !activeConfig.EnablePropagationNode {
		t.Fatalf("expected propagation enabled")
	}
	if len(activeConfig.ControlAllowedIdentities) != 2 {
		t.Fatalf("expected control identities, got %v", activeConfig.ControlAllowedIdentities)
	}
}

func TestSetRemotePathsDetectsDir(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config")
	os.WriteFile(configFile, []byte("[]"), 0o600)
	_, err := setRemotePaths(tmpDir)
	if err != nil {
		t.Fatalf("set remote paths: %v", err)
	}
	if configPath != filepath.Join(tmpDir, "config") {
		t.Fatalf("unexpected configPath %s", configPath)
	}
}
