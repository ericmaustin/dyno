package encoding

import (
	"fmt"
	"strings"
)

const (
	// FieldStructTagName is the struct fieldConfig used to denote a document field
	FieldStructTagName = "dyno"
)

// fieldConfig represents a field configuration
type fieldConfig struct {
	Name     string
	Append   string
	Prepend  string
	Skip     bool
	Embed    bool
	OmitNil  bool
	OmitZero bool
	JSON     bool
}

func parseTag(tagStr string) *fieldConfig {
	conf := &fieldConfig{}
	if len(tagStr) == 0 {
		return conf
	}
	if strings.Contains(tagStr, ",") {
		parts := strings.Split(tagStr, ",")
		if len(parts[0]) > 0 {
			switch parts[0] {
			case "*":
				conf.Embed = true
			case "-":
				conf.Skip = true
			default:
				conf.Name = parts[0]
			}
		}
		for i := 1; i < len(parts); i++ {
			if len(parts[i]) > 0 {
				parseTagPart(parts[i], conf)
			}
		}
	} else {
		conf.Name = tagStr
		switch conf.Name {
		case "*":
			conf.Embed = true
		case "-":
			conf.Skip = true
		}
	}
	if !conf.Embed && (len(conf.Append) > 0 || len(conf.Prepend) > 0) {
		panic(fmt.Errorf("append and prepend cannot be used if field is not embedded"))
	}
	return conf
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
	case "*", "embed":
		conf.Name = ""
		conf.Embed = true
	case "omitnil":
		conf.OmitNil = true
	case "omitzero":
		conf.OmitZero = true
	case "omitempty":
		conf.Name = ""
		conf.OmitNil = true
		conf.OmitZero = true
	case "-", "skip":
		conf.Name = ""
		conf.Skip = true
	case "json":
		conf.JSON = true
	}
}
