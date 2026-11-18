package cli

import (
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/relux-works/swarma-lease/internal/lease"
)

// TestLaunchAgentDefinitionUsesStableAbsoluteBinary proves the generated per-user agent uses the stable binary path, the default service command, and both requested launch policies.
func TestLaunchAgentDefinitionUsesStableAbsoluteBinary(t *testing.T) {
	home := filepath.Join(string(os.PathSeparator), "Users", "launch-agent-test")
	definition := launchAgentDefinitionForHome(home)
	wantBinary := filepath.Join(home, "Library", "Application Support", "swarma-lease", "bin", "swarma-lease")
	if !filepath.IsAbs(definition.Arguments[0]) || definition.Arguments[0] != wantBinary {
		t.Fatalf("program path = %q, want absolute %q", definition.Arguments[0], wantBinary)
	}
	if len(definition.Arguments) != 2 || definition.Arguments[1] != "serve" {
		t.Fatalf("program arguments = %#v, want binary and serve", definition.Arguments)
	}
	output, err := renderLaunchAgent(definition)
	if err != nil {
		t.Fatalf("render plist: %v", err)
	}
	decoder := xml.NewDecoder(strings.NewReader(string(output)))
	for {
		if _, err := decoder.Token(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("generated plist is not well-formed XML: %v", err)
		}
	}
	for _, expected := range []string{
		"<key>RunAtLoad</key>\n    <true/>",
		"<key>KeepAlive</key>\n    <true/>",
		"<string>" + wantBinary + "</string>",
	} {
		if !strings.Contains(string(output), expected) {
			t.Fatalf("generated plist is missing %q", expected)
		}
	}
}

// TestLaunchAgentCommandUsesSystemAccountHome proves non-macOS refusal and Darwin use of account-record home instead of HOME.
// The Darwin path check is limited to the current account.
func TestLaunchAgentCommandUsesSystemAccountHome(t *testing.T) {
	if runtime.GOOS != "darwin" {
		var stdout, stderr strings.Builder
		if code := Run([]string{"launch-agent"}, &stdout, &stderr); code != 1 {
			t.Fatalf("launch-agent on %s exit=%d, want 1", runtime.GOOS, code)
		}
		if stdout.Len() != 0 || !strings.Contains(stderr.String(), "launch-agent is available on macOS only") {
			t.Fatalf("non-macOS refusal stdout=%q stderr=%q", stdout.String(), stderr.String())
		}
		return
	}
	if os.Geteuid() == 0 {
		t.Skip("the service does not run as root")
	}
	t.Setenv("HOME", filepath.Join(string(os.PathSeparator), "Users", "untrusted-home"))
	home, err := lease.DefaultHomeDir()
	if err != nil {
		t.Fatalf("resolve system-account home: %v", err)
	}
	var stdout, stderr strings.Builder
	if code := Run([]string{"launch-agent"}, &stdout, &stderr); code != 0 {
		t.Fatalf("launch-agent exit=%d stderr=%q", code, stderr.String())
	}
	wantBinary := filepath.Join(home, "Library", "Application Support", "swarma-lease", "bin", "swarma-lease")
	if !strings.Contains(stdout.String(), "<string>"+wantBinary+"</string>") {
		t.Fatalf("generated plist does not use system-account path %q", wantBinary)
	}
}

// TestRenderLaunchAgentEscapesXMLAndRejectsInvalidText proves valid special path/environment characters are escaped and XML-forbidden control characters are refused.
func TestRenderLaunchAgentEscapesXMLAndRejectsInvalidText(t *testing.T) {
	definition := launchAgentDefinition{
		Label:     launchAgentLabel,
		Arguments: []string{"/Users/A&B <User>/Library/Application Support/swarma-lease/bin/swarma-lease", "serve"},
		Environment: map[string]string{
			"VALID_KEY": "left&<right>",
		},
	}
	output, err := renderLaunchAgent(definition)
	if err != nil {
		t.Fatalf("valid special characters were rejected: %v", err)
	}
	for _, escaped := range []string{
		"A&amp;B &lt;User&gt;",
		"left&amp;&lt;right&gt;",
	} {
		if !strings.Contains(string(output), escaped) {
			t.Errorf("generated plist is missing escaped text %q", escaped)
		}
	}
	decoder := xml.NewDecoder(strings.NewReader(string(output)))
	for {
		if _, err := decoder.Token(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("escaped plist is not well-formed XML: %v", err)
		}
	}

	for _, invalid := range []launchAgentDefinition{
		{Label: launchAgentLabel, Arguments: []string{"/Users/test/\x00/bin/swarma-lease", "serve"}},
		{Label: launchAgentLabel, Arguments: []string{"/Users/test/bin/swarma-lease", "serve"}, Environment: map[string]string{"VALID_KEY": "bad\x01value"}},
	} {
		if _, err := renderLaunchAgent(invalid); err == nil {
			t.Errorf("invalid XML text was accepted: %#v", invalid)
		}
	}
}

// TestLaunchAgentCommandRejectsExtraArguments proves the generator refuses an unsupported invocation without emitting a partial plist.
func TestLaunchAgentCommandRejectsExtraArguments(t *testing.T) {
	var stdout, stderr strings.Builder
	if code := Run([]string{"launch-agent", "--write"}, &stdout, &stderr); code != 2 {
		t.Fatalf("launch-agent with extra argument exit=%d, want 2", code)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage: swarma-lease launch-agent") {
		t.Fatalf("rejected invocation stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

// TestRenderLaunchAgentRejectsUnsafeDefinitions proves relative executables, malformed labels, and invalid environment keys refuse while a valid definition remains renderable.
func TestRenderLaunchAgentRejectsUnsafeDefinitions(t *testing.T) {
	valid := launchAgentDefinition{
		Label:     launchAgentLabel,
		Arguments: []string{"/Users/test/Library/Application Support/swarma-lease/bin/swarma-lease", "serve"},
	}
	if _, err := renderLaunchAgent(valid); err != nil {
		t.Fatalf("valid definition was rejected: %v", err)
	}
	invalid := []launchAgentDefinition{
		{Label: launchAgentLabel, Arguments: []string{"relative/swarma-lease", "serve"}},
		{Label: "../swarma-lease", Arguments: valid.Arguments},
		{Label: launchAgentLabel, Arguments: valid.Arguments, Environment: map[string]string{"BAD=KEY": "value"}},
	}
	for index, definition := range invalid {
		if _, err := renderLaunchAgent(definition); err == nil {
			t.Errorf("invalid definition %d was accepted", index)
		}
	}
}
