package dataset

import (
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-datastore"
)

// Transform is a record of executing a transformation on data.
// Transforms could be anything from an SQL query, a jupyter notebook, the state of an
// ETL pipeline, etc, so long as the input is one or more datasets, and the output
// is a single dataset.
// Transform should contain all the machine-necessary bits to deterministicly execute
// the process referenced in "Data".
// Consumers of Transforms may be able to produce abstract versions of
// transformations, a decision left to implementations
type Transform struct {
	// private storage for reference to this object
	path datastore.Key
	// Kind should always equal KindTransform
	Kind `json:"kind,omitempty"`
	// Syntax this transform was written in
	Syntax string `json:"syntax,omitempty"`
	// AppVersion is an identifier for the application and version number that produced the result
	AppVersion string
	// Data is the path to the process that produced this transformation.
	Data string `json:"data,omitempty"`

	// Structure is the output structure of this transformation
	Structure *Structure `json:"structure,omitempty"`

	// TODO - currently removing b/c I think this might be too strict.
	// Platform is an identifier for the operating system that performed the transform
	// Platform string `json:"platform,omitempty"`

	// Config outlines any configuration that would affect the resulting hash
	Config map[string]interface{}

	// Resources is a map of all datasets referenced in this transform, with alphabetical
	// keys generated by datasets in order of appearance within the transform.
	// all tables referred to in the transform should be present here
	// Keys are _always_ referenced in the form [a-z,aa-zz,aaa-zzz, ...] by order of appearance.
	// The transform itself is rewritten to refer to these table names using bind variables
	Resources map[string]*Dataset
}

// NewTransformRef creates a Transform pointer with the internal
// path property specified, and no other fields.
func NewTransformRef(path datastore.Key) *Transform {
	return &Transform{path: path}
}

// Path gives the internal path reference for this Transform
func (q *Transform) Path() datastore.Key {
	return q.path
}

// IsEmpty checks to see if transform has any fields other than the internal path
func (q *Transform) IsEmpty() bool {
	return q.Data == "" && q.Resources == nil
}

// Assign collapses all properties of a group of queries onto one.
// this is directly inspired by Javascript's Object.assign
func (q *Transform) Assign(qs ...*Transform) {
	for _, q2 := range qs {
		if q2 == nil {
			continue
		}
		if q2.Path().String() != "" {
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
		if q2.AppVersion != "" {
			q.AppVersion = q2.AppVersion
		}
		if q2.Kind != "" {
			q.Kind = q2.Kind
		}
		if q2.Structure != nil {
			if q.Structure == nil {
				q.Structure = &Structure{}
			}
			q.Structure.Assign(q2.Structure)
		}
		if q2.Data != "" {
			q.Data = q2.Data
		}
		if q2.Resources != nil {
			if q.Resources == nil {
				q.Resources = map[string]*Dataset{}
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
	AppVersion string                 `json:"appVersion,omitempty"`
	Config     map[string]interface{} `json:"config,omitempty"`
	Data       string                 `json:"data,omitempty"`
	Kind       Kind                   `json:"kind,omitempty"`
	Resources  map[string]*Dataset    `json:"resources,omitempty"`
	Structure  *Structure             `json:"structure,omitempty"`
	Syntax     string                 `json:"syntax,omitempty"`
}

// MarshalJSON satisfies the json.Marshaler interface
func (q Transform) MarshalJSON() ([]byte, error) {
	// if we're dealing with an empty object that has a path specified, marshal to a string instead
	if q.path.String() != "" && q.IsEmpty() {
		return q.path.MarshalJSON()
	}

	kind := q.Kind
	if kind == "" {
		kind = KindTransform
	}

	return json.Marshal(&_transform{
		AppVersion: q.AppVersion,
		Config:     q.Config,
		Data:       q.Data,
		Kind:       kind,
		Resources:  q.Resources,
		Structure:  q.Structure,
		Syntax:     q.Syntax,
	})
}

// UnmarshalJSON satisfies the json.Unmarshaler interface
func (q *Transform) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*q = Transform{path: datastore.NewKey(s)}
		return nil
	}

	_q := &_transform{}
	if err := json.Unmarshal(data, _q); err != nil {
		return err
	}

	*q = Transform{
		AppVersion: _q.AppVersion,
		Config:     _q.Config,
		Data:       _q.Data,
		Kind:       _q.Kind,
		Resources:  _q.Resources,
		Structure:  _q.Structure,
		Syntax:     _q.Syntax,
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
		return nil, fmt.Errorf("couldn't parse transform")
	}
}
