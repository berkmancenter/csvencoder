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
	delimiter    rune
	skipFields   []string
	headerFields []string
	buffer       *bytes.Buffer
	csvWriter    *csv.Writer
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
		e.skipFields = strings.Split(skip.(string), " ")
	}

	header, ok := conf["header_fields"]
	if ok && header.(string) != "" {
		e.headerFields = strings.Split(header.(string), " ")
	}

	e.buffer = new(bytes.Buffer)
	e.csvWriter = csv.NewWriter(e.buffer)
	e.csvWriter.Comma = e.delimiter
	e.csvWriter.Write(e.headerFields)

	return nil
}

func (e *CSVEncoder) Encode(pack *pipeline.PipelinePack) ([]byte, error) {
	row := []string{}
	fields := pack.Message.GetFields()

FieldLoop:
	for _, field := range fields {
		for _, skip := range e.skipFields {
			if field.GetName() == skip {
				continue FieldLoop
			}
		}
		row = append(row, fmt.Sprint(field.GetValue()))
	}
	e.csvWriter.Write(row)
	e.csvWriter.Flush()
	encoded := e.buffer.Bytes()
	e.buffer.Reset()
	return encoded, nil
}
