package csvencoder

import (
	"github.com/mozilla-services/heka/pipeline"

	"bytes"
	"encoding/csv"
	"fmt"
	"strings"
)

func init() {
	pipeline.RegisterPlugin("CSVEncoder",
		func() interface{} { return new(CSVEncoder) })
}

var DefaultDelimiter = ','

type CSVEncoder struct {
	delimiter   rune
	skip_fields []string
}

func (e *CSVEncoder) Init(config interface{}) error {
	conf := config.(pipeline.PluginConfig)

	delim, ok := conf["delimiter"]
	if ok {
		e.delimiter = delim.(rune)
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
	row := []string{}
	b := new(bytes.Buffer)
	fields := pack.Message.GetFields()
	csvWriter := csv.NewWriter(b)
	csvWriter.Comma = e.delimiter

FieldLoop:
	for _, field := range fields {
		for _, skip := range e.skip_fields {
			if field.GetName() == skip {
				continue FieldLoop
			}
		}
		row = append(row, fmt.Sprint(field.GetValue()))
	}
	csvWriter.Write(row)
	csvWriter.Flush()
	return b.Bytes(), nil
}
