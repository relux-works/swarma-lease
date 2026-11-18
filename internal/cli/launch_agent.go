package cli

import (
	"bytes"
	"embed"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/relux-works/swarma-lease/internal/lease"
)

//go:embed launchagent.plist.tmpl
var launchAgentTemplateFiles embed.FS

const launchAgentLabel = "works.relux.swarma-lease"

type launchAgentDefinition struct {
	Label                     string
	Arguments                 []string
	Environment               map[string]string
	RequiresBackgroundSession bool
}

func launchAgentDefinitionForHome(home string) launchAgentDefinition {
	binary := filepath.Join(home, "Library", "Application Support", "swarma-lease", "bin", "swarma-lease")
	return launchAgentDefinition{
		Label:     launchAgentLabel,
		Arguments: []string{binary, "serve"},
	}
}

func renderLaunchAgent(definition launchAgentDefinition) ([]byte, error) {
	if !validLaunchAgentLabel(definition.Label) || len(definition.Arguments) == 0 || !filepath.IsAbs(definition.Arguments[0]) {
		return nil, errors.New("invalid launch agent definition")
	}
	for _, argument := range definition.Arguments {
		if argument == "" || strings.ContainsRune(argument, '\x00') {
			return nil, errors.New("invalid launch agent definition")
		}
	}
	for key, value := range definition.Environment {
		if !validEnvironmentKey(key) || strings.ContainsRune(value, '\x00') {
			return nil, errors.New("invalid launch agent definition")
		}
	}

	templateBytes, err := launchAgentTemplateFiles.ReadFile("launchagent.plist.tmpl")
	if err != nil {
		return nil, fmt.Errorf("read launch agent template: %w", err)
	}
	label, err := escapeXMLText(definition.Label)
	if err != nil {
		return nil, fmt.Errorf("escape launch agent label: %w", err)
	}
	arguments := make([]string, 0, len(definition.Arguments))
	for _, argument := range definition.Arguments {
		escaped, err := escapeXMLText(argument)
		if err != nil {
			return nil, fmt.Errorf("escape launch agent argument: %w", err)
		}
		arguments = append(arguments, "        <string>"+escaped+"</string>")
	}
	var environment bytes.Buffer
	keys := make([]string, 0, len(definition.Environment))
	for key := range definition.Environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		escapedKey, err := escapeXMLText(key)
		if err != nil {
			return nil, fmt.Errorf("escape launch agent environment key: %w", err)
		}
		escapedValue, err := escapeXMLText(definition.Environment[key])
		if err != nil {
			return nil, fmt.Errorf("escape launch agent environment value: %w", err)
		}
		if environment.Len() > 0 {
			environment.WriteByte('\n')
		}
		environment.WriteString("        <key>" + escapedKey + "</key>\n        <string>" + escapedValue + "</string>")
	}

	plist := strings.Replace(string(templateBytes), "<string>__LABEL__</string>", "<string>"+label+"</string>", 1)
	plist = strings.Replace(plist, "        <string>__PROGRAM_ARGUMENTS__</string>", strings.Join(arguments, "\n"), 1)
	backgroundSessionLimit := ""
	if definition.RequiresBackgroundSession {
		backgroundSessionLimit = "    <key>LimitLoadToSessionType</key>\n    <string>Background</string>\n"
	}
	plist = strings.Replace(plist, "    __SESSION_TYPE_LIMIT__\n", backgroundSessionLimit, 1)
	if environment.Len() == 0 {
		plist = strings.Replace(plist, "    <key>EnvironmentVariables</key>\n    <dict>\n        <key>__ENVIRONMENT_KEY__</key>\n        <string>__ENVIRONMENT_VALUE__</string>\n    </dict>\n", "", 1)
	} else {
		plist = strings.Replace(plist, "        <key>__ENVIRONMENT_KEY__</key>\n        <string>__ENVIRONMENT_VALUE__</string>", environment.String(), 1)
	}
	if strings.Contains(plist, "__LABEL__") || strings.Contains(plist, "__PROGRAM_ARGUMENTS__") || strings.Contains(plist, "__ENVIRONMENT_") || strings.Contains(plist, "__SESSION_TYPE_LIMIT__") {
		return nil, errors.New("launch agent template has an unreplaced placeholder")
	}
	return []byte(plist), nil
}

func escapeXMLText(value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", errors.New("invalid UTF-8 in plist text")
	}
	for _, character := range value {
		if character != '\t' && character != '\n' && character != '\r' && !(character >= 0x20 && character <= 0xD7FF || character >= 0xE000 && character <= 0xFFFD || character >= 0x10000 && character <= 0x10FFFF) {
			return "", errors.New("invalid XML character in plist text")
		}
	}
	var escaped bytes.Buffer
	if err := xml.EscapeText(&escaped, []byte(value)); err != nil {
		return "", err
	}
	return escaped.String(), nil
}

func validLaunchAgentLabel(label string) bool {
	if label == "" || strings.HasPrefix(label, ".") || strings.HasPrefix(label, "-") {
		return false
	}
	for _, character := range label {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '.' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func validEnvironmentKey(key string) bool {
	if key == "" {
		return false
	}
	for index, character := range key {
		valid := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || character == '_'
		if index > 0 {
			valid = valid || (character >= '0' && character <= '9')
		}
		if !valid {
			return false
		}
	}
	return true
}

func writeLaunchAgent(stdout, stderr io.Writer) int {
	if runtime.GOOS != "darwin" {
		_, _ = fmt.Fprintln(stderr, "launch-agent is available on macOS only")
		return 1
	}
	home, err := lease.DefaultHomeDir()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "launch-agent: resolve system-account home: %v\n", err)
		return 1
	}
	output, err := renderLaunchAgent(launchAgentDefinitionForHome(home))
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "launch-agent: render plist: %v\n", err)
		return 1
	}
	if _, err := stdout.Write(output); err != nil {
		_, _ = fmt.Fprintf(stderr, "launch-agent: write plist: %v\n", err)
		return 1
	}
	return 0
}
