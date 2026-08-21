package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const maxConfigNesting = 64

func rejectDuplicateJSONKeys(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	var walk func(int) error
	walk = func(depth int) error {
		token, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		if depth >= maxConfigNesting {
			return fmt.Errorf("configuration nesting exceeds %d levels", maxConfigNesting)
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				keyToken, err := d.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("object key is not a string")
				}
				if seen[key] {
					return fmt.Errorf("duplicate key %q", key)
				}
				seen[key] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = d.Token()
			return err
		case '[':
			for d.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = d.Token()
			return err
		default:
			return fmt.Errorf("unexpected delimiter %q", delim)
		}
	}
	if err := walk(0); err != nil {
		return err
	}
	if d.More() {
		return errors.New("multiple JSON values")
	}
	return nil
}

// decodeSimpleYAML intentionally implements only the conservative YAML subset emitted and
// documented by Dark Factory: indentation-based mappings, scalar sequences, JSON-style
// quoted strings, booleans, integers, and []. Anchors, aliases, tags, flow maps, tabs,
// multiline scalars, merge keys, and multiple documents are rejected.
func decodeSimpleYAML(raw []byte) ([]byte, error) {
	type line struct {
		indent int
		text   string
		number int
	}
	var lines []line
	for i, original := range strings.Split(string(raw), "\n") {
		if strings.ContainsRune(original, '\t') {
			return nil, fmt.Errorf("line %d: tabs are not allowed", i+1)
		}
		trimmed := strings.TrimSpace(original)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.ContainsAny(trimmed, "&*!|>") || trimmed == "---" || trimmed == "..." || strings.HasPrefix(trimmed, "<<:") {
			return nil, fmt.Errorf("line %d: unsupported YAML feature", i+1)
		}
		indent := len(original) - len(strings.TrimLeft(original, " "))
		if indent%2 != 0 {
			return nil, fmt.Errorf("line %d: indentation must use multiples of two spaces", i+1)
		}
		lines = append(lines, line{indent: indent, text: strings.TrimSpace(original), number: i + 1})
	}
	if len(lines) == 0 {
		return nil, errors.New("configuration is empty")
	}
	var parseBlock func(index, indent, depth int) (any, int, error)
	parseBlock = func(index, indent, depth int) (any, int, error) {
		if depth >= maxConfigNesting {
			return nil, index, fmt.Errorf("configuration nesting exceeds %d levels", maxConfigNesting)
		}
		if index >= len(lines) || lines[index].indent != indent {
			return nil, index, errors.New("invalid indentation")
		}
		isList := strings.HasPrefix(lines[index].text, "- ")
		if isList {
			var values []any
			for index < len(lines) && lines[index].indent == indent {
				current := lines[index]
				if !strings.HasPrefix(current.text, "- ") {
					return nil, index, fmt.Errorf("line %d: mixed mapping and sequence", current.number)
				}
				value, err := yamlScalar(strings.TrimSpace(strings.TrimPrefix(current.text, "- ")))
				if err != nil {
					return nil, index, fmt.Errorf("line %d: %w", current.number, err)
				}
				values = append(values, value)
				index++
			}
			return values, index, nil
		}
		values := map[string]any{}
		for index < len(lines) && lines[index].indent == indent {
			current := lines[index]
			if strings.HasPrefix(current.text, "- ") {
				return nil, index, fmt.Errorf("line %d: mixed mapping and sequence", current.number)
			}
			colon := strings.Index(current.text, ":")
			if colon <= 0 {
				return nil, index, fmt.Errorf("line %d: expected key: value", current.number)
			}
			key := strings.TrimSpace(current.text[:colon])
			rest := strings.TrimSpace(current.text[colon+1:])
			if key == "" || strings.ContainsAny(key, " {}[],'\"") {
				return nil, index, fmt.Errorf("line %d: invalid key", current.number)
			}
			if _, exists := values[key]; exists {
				return nil, index, fmt.Errorf("duplicate key %q", key)
			}
			index++
			if rest == "" {
				if index >= len(lines) || lines[index].indent != indent+2 {
					return nil, index, fmt.Errorf("line %d: empty mapping value", current.number)
				}
				child, next, err := parseBlock(index, indent+2, depth+1)
				if err != nil {
					return nil, index, err
				}
				values[key] = child
				index = next
			} else {
				value, err := yamlScalar(rest)
				if err != nil {
					return nil, index, fmt.Errorf("line %d: %w", current.number, err)
				}
				values[key] = value
			}
			if index < len(lines) && lines[index].indent < indent {
				break
			}
			if index < len(lines) && lines[index].indent > indent {
				return nil, index, fmt.Errorf("line %d: unexpected indentation", lines[index].number)
			}
		}
		return values, index, nil
	}
	value, next, err := parseBlock(0, lines[0].indent, 0)
	if err != nil {
		return nil, err
	}
	if lines[0].indent != 0 || next != len(lines) {
		return nil, errors.New("top-level mapping must start at column one")
	}
	return json.Marshal(value)
}

func yamlScalar(raw string) (any, error) {
	if raw == "[]" {
		return []any{}, nil
	}
	if raw == "{}" {
		return map[string]any{}, nil
	}
	if strings.HasPrefix(raw, "[") || strings.HasPrefix(raw, "{") {
		return nil, errors.New("flow collections are not supported except [] and {}")
	}
	if raw == "true" {
		return true, nil
	}
	if raw == "false" {
		return false, nil
	}
	if raw == "null" || raw == "~" {
		return nil, nil
	}
	if strings.HasPrefix(raw, "\"") {
		var value string
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return nil, errors.New("invalid quoted string")
		}
		return value, nil
	}
	if strings.HasPrefix(raw, "'") {
		if len(raw) < 2 || !strings.HasSuffix(raw, "'") {
			return nil, errors.New("invalid single-quoted string")
		}
		return strings.ReplaceAll(raw[1:len(raw)-1], "''", "'"), nil
	}
	if strings.Contains(raw, " #") {
		raw = strings.TrimSpace(strings.SplitN(raw, " #", 2)[0])
	}
	if value, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return value, nil
	}
	if strings.ContainsAny(raw, "{}[],") {
		return nil, errors.New("unsupported unquoted scalar")
	}
	return raw, nil
}
