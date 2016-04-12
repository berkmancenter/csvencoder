package csvencoder

import (
	"github.com/mozilla-services/heka/pipeline"

	"fmt"
	"strings"
)

func init() {
	pipeline.RegisterPlugin("CSVEncoder",
		func() interface{} { return new(CSVEncoder) })
}

var DefaultDelimiter = ","

type CSVEncoder struct {
	delimiter   string
	skip_fields []string
}

func (e *CSVEncoder) Init(config interface{}) error {
	conf := config.(pipeline.PluginConfig)

	delim, ok := conf["delimiter"]
	if ok {
		e.delimiter = delim.(string)
	} else {
		e.delimiter = DefaultDelimiter
	}

	skip, ok := conf["skip_fields"]
	if ok && skip.(string) != "" {
		e.skip_fields = strings.Split(skip.(string), " ")
	}

	return nil
}

func (e *CSVEncoder) Encode(pack *pipeline.PipelinePack) ([]byte, error) {
	var row []byte
	fields := pack.Message.GetFields()

FieldLoop:
	for i, field := range fields {
		for _, skip := range e.skip_fields {
			if field.GetName() == skip {
				continue FieldLoop
			}
		}
		row = append(row, fmt.Sprint(field.GetValue())...)
		if i < len(fields)-1 {
			row = append(row, e.delimiter...)
		}
	}
	row = append(row, "\n"...)
	return row, nil
}
