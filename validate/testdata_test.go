package validate

import (
	"github.com/qri-io/dataset"
)

var emptyRawText = ``

// has lazy quotes
var rawText1 = `first_name,last_name,username,age
"Rob","Pike",rob, 100
Ken,Thompson,ken, 75.5
"Robert","Griesemer","gri", 100`

var namesStructure = &dataset.Structure{
	Format: "csv",
	FormatConfig: map[string]interface{}{
		"headerRow": true,
	},
	Schema: map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type": "array",
			"items": []interface{}{
				map[string]interface{}{"title": "first_name", "type": "string"},
				map[string]interface{}{"title": "last_name", "type": "string"},
				map[string]interface{}{"title": "username", "type": "string"},
				map[string]interface{}{"title": "age", "type": "integer"},
			},
		},
	},
}

// has nonNumeric quotes and comma inside quotes on last line
var rawText2 = `"first_name","last_name","username","age"
"Rob","Pike","rob", 22
"Robert","Griesemer","gri", 100
"abc","def,ghi","jkl",1000`

// same as above but with spaces in last line
var rawText2b = `"first_name","last_name","username","age"
"Rob","Pike","rob", 22
"Robert","Griesemer","gri", 100
"abc", "def,ghi", "jkl", 1000`

// error in last row "age" column
var rawText2c = `first_name,last_name,username,age
"Rob","Pike","rob",22
"Robert","Griesemer","gri",100
"abc","def,ghi","jkl",_`

// NOTE: technically this is valid csv and we should be catching this at an earlier filter
var rawText3 = `<html>
<body>
<table>
<th>
<tr>col</tr>
</th>
</table>
</body>
</html>`

var rawText4 = `<html>
<body>
<table>
<th>
<tr>Last Name, First</tr>
<tr>
</th>
</table>
</body>
</html>`
