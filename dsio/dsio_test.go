package dsio

import (
	"bytes"
	"testing"

	"github.com/qri-io/dataset"
)

func TestNewEntryReader(t *testing.T) {
	cases := []struct {
		st  *dataset.Structure
		err string
	}{
		{&dataset.Structure{}, "structure must have a data format"},
		{&dataset.Structure{Format: "cbor", Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: "json", Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: "csv", Schema: dataset.BaseSchemaArray}, ""},
	}

	for i, c := range cases {
		_, err := NewEntryReader(c.st, &bytes.Buffer{})
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

func TestNewEntryWriter(t *testing.T) {
	cases := []struct {
		st  *dataset.Structure
		err string
	}{
		{&dataset.Structure{}, "structure must have a data format"},
		{&dataset.Structure{Format: "cbor", Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: "json", Schema: dataset.BaseSchemaArray}, ""},
		{&dataset.Structure{Format: "csv", Schema: dataset.BaseSchemaArray}, ""},
	}

	for i, c := range cases {
		_, err := NewEntryWriter(c.st, &bytes.Buffer{})
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}
}

func TestReadAll(t *testing.T) {
	r, err := NewIdentityReader(&dataset.Structure{Format: "native", Schema: dataset.BaseSchemaArray}, []interface{}{"a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadAll(r); err != nil {
		t.Error(err)
	}
}
