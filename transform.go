package dataset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Transform is a record of executing a transformation on data.
// Transforms can theoretically be anything from an SQL query, a jupyter
// notebook, the state of an ETL pipeline, etc, so long as the input is zero or
// more datasets, and the output is a single dataset
// Ideally, transforms should contain all the machine-necessary bits to
// deterministicly execute the algorithm referenced in "ScriptPath".
type Transform struct {
	// private storage for reference to this object
	path string

	// Kind should always equal KindTransform
	Qri Kind `json:"qri,omitempty"`
	// Script is a reader of raw script data
	Script io.Reader `json:"_"`
	// ScriptPath is the path to the script that produced this transformation.
	ScriptPath string `json:"script,omitempty"`
	// Syntax this transform was written in
	Syntax string `json:"syntax,omitempty"`
	// SyntaxVersion is an identifier for the application and version number that
	// produced the result
	SyntaxVersion string
	// Structure is the output structure of this transformation
	Structure *Structure `json:"structure,omitempty"`
	// Config outlines any configuration that would affect the resulting hash
	Config map[string]interface{}
	// Resources is a map of all datasets referenced in this transform, with
	// alphabetical keys generated by datasets in order of appearance within the
	// transform
	Resources map[string]*TransformResource
}

// TransformResource describes an external data dependency, the prime use case
// is for importing other datasets, but in the future this may be expanded to
// include details that specify resources other than datasets (urls?), and
// details for interpreting the resource (eg. a selector to specify only a
// subset of a resource is required)
type TransformResource struct {
	Path string `json:"path"`
}

// private version for marshalling purposes only
type transformResource TransformResource

// UnmarshalJSON implements json.Unmarshaler, allowing both string and object
// representations
func (r *TransformResource) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*r = TransformResource{Path: s}
		return nil
	}

	_r := &transformResource{}
	if err := json.Unmarshal(data, _r); err != nil {
		return err
	}

	*r = TransformResource(*_r)
	return nil
}

// NewTransformRef creates a Transform pointer with the internal
// path property specified, and no other fields.
func NewTransformRef(path string) *Transform {
	return &Transform{path: path}
}

// Path gives the internal path reference for this Transform
func (q *Transform) Path() string {
	return q.path
}

// IsEmpty checks to see if transform has any fields other than the internal path
func (q *Transform) IsEmpty() bool {
	return q.ScriptPath == "" &&
		q.Resources == nil &&
		q.Syntax == "" &&
		q.SyntaxVersion == "" &&
		q.Structure == nil &&
		q.Config == nil
}

// SetPath sets the internal path property of a Transform
// Use with caution. most callers should never need to call SetPath
func (q *Transform) SetPath(path string) {
	q.path = path
}

// Assign collapses all properties of a group of queries onto one.
// this is directly inspired by Javascript's Object.assign
func (q *Transform) Assign(qs ...*Transform) {
	for _, q2 := range qs {
		if q2 == nil {
			continue
		}
		if q2.Path() != "" {
			q.path = q2.path
		}
		if q2.Syntax != "" {
			q.Syntax = q2.Syntax
		}
		if q2.Config != nil {
			if q.Config == nil {
				q.Config = map[string]interface{}{}
			}
			for key, val := range q2.Config {
				q.Config[key] = val
			}
		}
		if q2.SyntaxVersion != "" {
			q.SyntaxVersion = q2.SyntaxVersion
		}
		if q2.Qri != "" {
			q.Qri = q2.Qri
		}
		if q2.Structure != nil {
			if q.Structure == nil {
				q.Structure = &Structure{}
			}
			q.Structure.Assign(q2.Structure)
		}
		if q2.ScriptPath != "" {
			q.ScriptPath = q2.ScriptPath
		}
		if q2.Resources != nil {
			if q.Resources == nil {
				q.Resources = map[string]*TransformResource{}
			}
			for key, val := range q2.Resources {
				q.Resources[key] = val
			}
		}
	}
}

// _transform is a private struct for marshaling into & out of.
// fields must remain sorted in lexographical order
type _transform struct {
	Config        map[string]interface{}        `json:"config,omitempty"`
	Qri           Kind                          `json:"qri,omitempty"`
	Resources     map[string]*TransformResource `json:"resources,omitempty"`
	ScriptPath    string                        `json:"scriptPath,omitempty"`
	Structure     *Structure                    `json:"structure,omitempty"`
	Syntax        string                        `json:"syntax,omitempty"`
	SyntaxVersion string                        `json:"syntaxVersion,omitempty"`
}

// MarshalJSON satisfies the json.Marshaler interface
func (q Transform) MarshalJSON() ([]byte, error) {
	// if we're dealing with an empty object that has a path specified, marshal to a string instead
	if q.path != "" && q.IsEmpty() {
		return json.Marshal(q.path)
	}
	return q.MarshalJSONObject()
}

// MarshalJSONObject always marshals to a json Object, even if meta is empty or a reference
func (q Transform) MarshalJSONObject() ([]byte, error) {
	kind := q.Qri
	if kind == "" {
		kind = KindTransform
	}

	return json.Marshal(&_transform{
		SyntaxVersion: q.SyntaxVersion,
		Config:        q.Config,
		Qri:           kind,
		Resources:     q.Resources,
		ScriptPath:    q.ScriptPath,
		Structure:     q.Structure,
		Syntax:        q.Syntax,
	})
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (q *Transform) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*q = Transform{path: s}
		return nil
	}

	_q := &_transform{}
	if err := json.Unmarshal(data, _q); err != nil {
		return err
	}

	*q = Transform{
		SyntaxVersion: _q.SyntaxVersion,
		Config:        _q.Config,
		Qri:           _q.Qri,
		Resources:     _q.Resources,
		ScriptPath:    _q.ScriptPath,
		Structure:     _q.Structure,
		Syntax:        _q.Syntax,
	}
	return nil
}

// UnmarshalTransform tries to extract a resource type from an empty
// interface. Pairs nicely with datastore.Get() from github.com/ipfs/go-datastore
func UnmarshalTransform(v interface{}) (*Transform, error) {
	switch q := v.(type) {
	case *Transform:
		return q, nil
	case Transform:
		return &q, nil
	case []byte:
		transform := &Transform{}
		err := json.Unmarshal(q, transform)
		return transform, err
	default:
		err := fmt.Errorf("couldn't parse transform")
		log.Debug(err.Error())
		return nil, err
	}
}

// Encode creates a TransformPod from a Transform instance
func (q Transform) Encode() *TransformPod {
	ct := &TransformPod{
		SyntaxVersion: q.SyntaxVersion,
		Config:        q.Config,
		ScriptPath:    q.ScriptPath,
		Path:          q.Path(),
		Qri:           q.Qri.String(),
		Syntax:        q.Syntax,
	}

	if q.Resources != nil {
		ct.Resources = map[string]interface{}{}
		for key, r := range q.Resources {
			ct.Resources[key] = r
		}
	}

	if q.Structure != nil {
		ct.Structure = q.Structure.Encode()
	}

	return ct
}

// Decode creates a Transform from a TransformPod instance
func (q *Transform) Decode(ct *TransformPod) error {
	t := Transform{
		SyntaxVersion: ct.SyntaxVersion,
		Config:        ct.Config,
		ScriptPath:    ct.ScriptPath,
		path:          ct.Path,
		Syntax:        ct.Syntax,
	}

	if ct.Qri != "" {
		t.Qri = KindTransform
	}

	if ct.ScriptBytes != nil {
		t.Script = bytes.NewReader(ct.ScriptBytes)
	}

	if ct.Resources != nil {
		t.Resources = map[string]*TransformResource{}
		for key, rsc := range ct.Resources {
			switch v := rsc.(type) {
			case string:
				t.Resources[key] = &TransformResource{Path: v}
			default:
				// TODO - falling back to double marshalling is slow
				data, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("resource '%s': %s", key, err)
				}

				r := &TransformResource{}
				if err := json.Unmarshal(data, r); err != nil {
					return fmt.Errorf("resource '%s': %s", key, err)
				}
				t.Resources[key] = r
			}
		}
	}

	if ct.Structure != nil {
		t.Structure = &Structure{}
		if err := t.Structure.Decode(ct.Structure); err != nil {
			return err
		}
	}

	*q = t
	return nil
}

// TransformPod is a variant of Transform safe for serialization (encoding & decoding)
// to static formats. It uses only simple go types
type TransformPod struct {
	Config        map[string]interface{} `json:"config,omitempty"`
	TransformPath string                 `json:"transformPath,omitempty"`
	Path          string                 `json:"path,omitempty"`
	Qri           string                 `json:"qri,omitempty"`
	Resources     map[string]interface{} `json:"resources,omitempty"`
	// Secrets doesn't exsit on Transform, only here for select use cases
	Secrets    map[string]string `json:"secrets,omitempty"`
	Structure  *StructurePod     `json:"structure,omitempty"`
	ScriptPath string            `json:"scriptPath,omitempty"`
	// ScriptBytes is for representing a script as a slice of bytes
	ScriptBytes   []byte `json:"scriptBytes,omitempty"`
	Syntax        string `json:"syntax,omitempty"`
	SyntaxVersion string `json:"syntaxVersion,omitempty"`
}

// Assign collapses all properties of zero or more TransformPod onto one.
// inspired by Javascript's Object.assign
func (tp *TransformPod) Assign(tps ...*TransformPod) {
	for _, t := range tps {
		if t == nil {
			continue
		}

		if t.Config != nil {
			tp.Config = t.Config
		}
		if t.TransformPath != "" {
			tp.TransformPath = t.TransformPath
		}
		if t.Path != "" {
			tp.Path = t.Path
		}
		if t.Qri != "" {
			tp.Qri = t.Qri
		}
		if t.Resources != nil {
			tp.Resources = t.Resources
		}
		if t.Secrets != nil {
			tp.Secrets = t.Secrets
		}

		// TODO - we should depricate the Structure field. it doesn't make sense anymore
		// if t.Structure != nil {
		// 	tp.Structure = t.Structure
		// }

		if t.ScriptPath != "" {
			tp.ScriptPath = t.ScriptPath
		}
		if t.ScriptBytes != nil {
			tp.ScriptBytes = t.ScriptBytes
		}
		if t.Syntax != "" {
			tp.Syntax = t.Syntax
		}
		if t.SyntaxVersion != "" {
			tp.SyntaxVersion = t.SyntaxVersion
		}
	}
}
