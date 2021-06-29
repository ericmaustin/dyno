package encoding

import (
	"errors"
	"strings"
)

const (
	// FieldStructTagName is the struct fieldConfig used to denote a document field
	FieldStructTagName = "dynamodbav"
)

const (
	_ = iota
	MarshalAsJSON
	MarshalAsYAML
	MarshalAsTOML
)

// fieldConfig represents a field configuration
type fieldConfig struct {
	Name      string
	Append    string
	Prepend   string
	Skip      bool
	Embed     bool
	MarshalAs int
}

func parseTag(tagStr string) (*fieldConfig, error) {
	conf := new(fieldConfig)
	if len(tagStr) == 0 {
		return conf, nil
	}

	parts := strings.Split(tagStr, ",")

	switch parts[0] {
	case "*":
		conf.Embed = true
	case "-":
		conf.Skip = true
	default:
		conf.Name = parts[0]
	}

	if len(parts) < 1 {
		return conf, nil
	}

	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parseTagPart(parts[i], conf)
		}
	}

	if !conf.Embed && (len(conf.Append) > 0 || len(conf.Prepend) > 0) {
		return nil, errors.New("append and prepend cannot be used if field is not embedded")
	}

	return conf, nil
}

func parseTagPart(part string, conf *fieldConfig) {
	if strings.Contains(part, "=") {
		subParts := strings.Split(part, "=")
		switch strings.ToLower(strings.TrimSpace(subParts[0])) {
		case "append":
			conf.Append = subParts[1]
		case "prepend":
			conf.Prepend = subParts[1]
		}

		return
	}

	switch strings.ToLower(strings.TrimSpace(part)) {
	case "*":
		conf.Embed = true
	case "-":
		conf.Skip = true
	case "json":
		conf.MarshalAs = MarshalAsJSON
	case "toml":
		conf.MarshalAs = MarshalAsTOML
	case "yaml":
		conf.MarshalAs = MarshalAsYAML
	}
}
