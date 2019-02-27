package stats

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

type TestCase struct {
	Description string
	JSONSchema  string
	JSONInput   string
	Expect      interface{}
}

func TestBasicStatsReader(t *testing.T) {
	allTypesIdentitySchemaArray := TestCase{
		"all types identity schema array of object entries",
		`{"type":"array"}`,
		`[
			{"int": 1, "float": 1.1, "nil": null, "bool": false, "string": "a"},
			{"int": 2, "float": 2.2, "nil": null, "bool": true, "string": "aa"},
			{"int": 3, "float": 3.3, "nil": null, "bool": false, "string": "aaa"},
			{"int": 4, "float": 4.4, "nil": null, "bool": true, "string": "aaaa"},
			{"int": 5, "float": 5.5, "nil": null, "bool": false, "string": "aaaaa"}
		]`,
		map[string]interface{}{
			"int": map[string]interface{}{
				"count": 5,
				"min":   float64(1),
				"max":   float64(5),
			},
			"float": map[string]interface{}{
				"count": 5,
				"min":   float64(1.1),
				"max":   float64(5.5),
			},
			"nil": map[string]interface{}{
				"count": 5,
			},
			"bool": map[string]interface{}{
				"count":      5,
				"trueCount":  2,
				"falseCount": 3,
			},
			"string": map[string]interface{}{
				"count":     5,
				"minLength": 1,
				"maxLength": 5,
			},
		},
	}

	allTypesIdentitySchemaObject := TestCase{
		"all types identity schema object of array entries",
		`{"type":"object"}`,
		`{
			"a" : [1,1.1,null,false,"a"],
			"b" : [2,2.2,null,true,"aa"],
			"c" : [3,3.3,null,false,"aaa"],
			"d" : [4,4.4,null,true,"aaaa"],
			"e" : [5,5.5,null,false,"aaaaa"]
		}`,
		[]interface{}{
			map[string]interface{}{
				"count": 5,
				"min":   float64(1),
				"max":   float64(5),
			},
			map[string]interface{}{
				"count": 5,
				"min":   float64(1.1),
				"max":   float64(5.5),
			},
			map[string]interface{}{
				"count": 5,
			},
			map[string]interface{}{
				"count":      5,
				"trueCount":  2,
				"falseCount": 3,
			},
			map[string]interface{}{
				"count":     5,
				"minLength": 1,
				"maxLength": 5,
			},
		},
	}

	RunTestCases(t, []TestCase{allTypesIdentitySchemaArray, allTypesIdentitySchemaObject})
}

func RunTestCases(t *testing.T, cases []TestCase) {
	for i, c := range cases {
		var sch map[string]interface{}
		if err := json.Unmarshal([]byte(c.JSONSchema), &sch); err != nil {
			t.Errorf("%d. %s error decoding schema: %s", i, c.Description, err)
			continue
		}
		st := &dataset.Structure{
			Format: "json",
			Schema: sch,
		}
		if c.JSONInput[0] == '{' {
			st.Schema = dataset.BaseSchemaObject
		}
		r, err := dsio.NewJSONReader(st, strings.NewReader(c.JSONInput))
		if err != nil {
			t.Errorf("%d. %s error creating json reader: %s", i, c.Description, err)
			continue
		}
		bsg, err := NewBasicStatsGenerator(r)
		if err != nil {
			t.Errorf("%d. %s error creating stats generator: %s", i, c.Description, err)
			continue
		}
		_, err = dsio.ReadAll(bsg)
		got := bsg.Stats()
		if !reflect.DeepEqual(c.Expect, got) {
			t.Errorf("%d. %s result stats mismatch", i, c.Description)
			expect, _ := json.Marshal(c.Expect)
			t.Logf("expected: %s\n", string(expect))
			got, _ := json.Marshal(got)
			t.Logf("     got: %s\n", string(got))
		}
	}
}
