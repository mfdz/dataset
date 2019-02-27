//Package stats calculates stats
package stats

import (
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

func NewBasicStatsGenerator(r dsio.EntryReader) (*BasicStatsGenerator, error) {
	// tlt, err := dsio.GetTopLevelType(r.Structure())
	// if err != nil {
	// 	return nil, err
	// }

	// var topStat statGenerator
	// if tlt == "object" {
	// 	topStat = &objectStatGenerator{}
	// } else {
	// 	topStat = &arrayStatGenerator{}
	// }

	return &BasicStatsGenerator{r: r}, nil
}

// BasicStatsGenerator wraps a dsio.EntryReader, on each call to read stats will update
// it's internal statistics
type BasicStatsGenerator struct {
	r     dsio.EntryReader
	stats statGenerator
}

func (r *BasicStatsGenerator) Stats() interface{} {
	return r.stats.Stats()
}

// Structure gives the structure being read
func (r *BasicStatsGenerator) Structure() *dataset.Structure {
	return r.r.Structure()
}

// ReadEntry reads one row of structured data from the reader
func (r *BasicStatsGenerator) ReadEntry() (dsio.Entry, error) {
	ent, err := r.r.ReadEntry()
	if err != nil {
		return ent, err
	}
	if r.stats == nil {
		r.stats = newStatGenerator(ent.Value)
	}
	r.stats.AddEntry(ent)
	return ent, nil
}

// Close finalizes the Reader
func (r *BasicStatsGenerator) Close() error {
	r.stats.Close()
	return r.r.Close()
}

type Stats interface {
	Stats() interface{}
}

type statGenerator interface {
	Stats
	AddEntry(dsio.Entry)
	Close() error
}

func newStatGenerator(val interface{}) statGenerator {
	switch val.(type) {
	default:
		return &nullStatGenerator{}
	case float64, float32:
		return &numericStatGenerator{typ: "number", max: float64(minInt), min: float64(maxInt)}
	case int:
		return &numericStatGenerator{typ: "integer", max: float64(minInt), min: float64(maxInt)}
	case string:
		return &stringStatGenerator{maxLength: minInt, minLength: maxInt}
	case bool:
		return &boolStatGenerator{}
	case map[string]interface{}:
		return &objectStatGenerator{children: map[string]statGenerator{}}
	case []interface{}:
		return &arrayStatGenerator{}
	}
}

type objectStatGenerator struct {
	children map[string]statGenerator
}

func (osg *objectStatGenerator) AddEntry(e dsio.Entry) {
	if mapEntry, ok := e.Value.(map[string]interface{}); ok {
		for key, val := range mapEntry {
			if _, ok := osg.children[key]; !ok {
				osg.children[key] = newStatGenerator(val)
			}
			osg.children[key].AddEntry(dsio.Entry{Key: key, Value: val})
		}
	}
}

func (osg *objectStatGenerator) Stats() interface{} {
	vals := map[string]interface{}{}
	for key, val := range osg.children {
		vals[key] = val.Stats()
	}
	return vals
}

func (osg *objectStatGenerator) Close() error {
	for _, val := range osg.children {
		if err := val.Close(); err != nil {
			return err
		}
	}
	return nil
}

type arrayStatGenerator struct {
	children []statGenerator
}

func (asg *arrayStatGenerator) AddEntry(e dsio.Entry) {
	if arrayEntry, ok := e.Value.([]interface{}); ok {
		for i, val := range arrayEntry {
			if len(asg.children) == i {
				asg.children = append(asg.children, newStatGenerator(val))
			}
			asg.children[i].AddEntry(dsio.Entry{Index: i, Value: val})
		}
	}
}

func (asg *arrayStatGenerator) Stats() interface{} {
	vals := make([]interface{}, len(asg.children))
	for i, val := range asg.children {
		vals[i] = val.Stats()
	}
	return vals
}

func (asg *arrayStatGenerator) Close() error {
	for _, val := range asg.children {
		if err := val.Close(); err != nil {
			return err
		}
	}
	return nil
}

const maxUint = ^uint(0)
const maxInt = int(maxUint >> 1)
const minInt = -maxInt - 1

type numericStatGenerator struct {
	typ   string
	count int
	min   float64
	max   float64
}

func (nsg *numericStatGenerator) AddEntry(e dsio.Entry) {
	var v float64
	switch x := e.Value.(type) {
	case int:
		v = float64(x)
	case float32:
		v = float64(x)
	case float64:
		v = x
	default:
		return
	}

	nsg.count++
	if v > nsg.max {
		nsg.max = v
	}
	if v < nsg.min {
		nsg.min = v
	}
}

func (nsg *numericStatGenerator) Stats() interface{} {
	if nsg.count == 0 {
		// avoid reporting default max/min figures, if count is above 0
		// at least one entry has been checked
		return map[string]interface{}{"count": 0}
	}
	return map[string]interface{}{
		"count": nsg.count,
		"min":   nsg.min,
		"max":   nsg.max,
	}
}

func (nsg *numericStatGenerator) Close() error {
	return nil
}

type stringStatGenerator struct {
	count     int
	minLength int
	maxLength int
}

func (ssg *stringStatGenerator) AddEntry(e dsio.Entry) {
	if str, ok := e.Value.(string); ok {
		ssg.count++
		if len(str) < ssg.minLength {
			ssg.minLength = len(str)
		}
		if len(str) > ssg.maxLength {
			ssg.maxLength = len(str)
		}
	}
}

func (ssg *stringStatGenerator) Stats() interface{} {
	if ssg.count == 0 {
		// avoid reporting default max/min figures, if count is above 0
		// at least one entry has been checked
		return map[string]interface{}{"count": 0}
	}
	return map[string]interface{}{
		"count":     ssg.count,
		"minLength": ssg.minLength,
		"maxLength": ssg.maxLength,
	}
}

func (ssg *stringStatGenerator) Close() error {
	return nil
}

type boolStatGenerator struct {
	count      int
	trueCount  int
	falseCount int
}

func (bsg *boolStatGenerator) AddEntry(e dsio.Entry) {
	if b, ok := e.Value.(bool); ok {
		bsg.count++
		if b {
			bsg.trueCount++
		} else {
			bsg.falseCount++
		}
	}
}

func (bsg *boolStatGenerator) Stats() interface{} {
	return map[string]interface{}{
		"count":      bsg.count,
		"trueCount":  bsg.trueCount,
		"falseCount": bsg.falseCount,
	}
}

func (bsg *boolStatGenerator) Close() error {
	return nil
}

type nullStatGenerator struct {
	count int
}

func (nsg *nullStatGenerator) AddEntry(e dsio.Entry) {
	if e.Value == nil {
		nsg.count++
	}
}
func (nsg *nullStatGenerator) Stats() interface{} {
	return map[string]interface{}{"count": nsg.count}
}
func (nsg *nullStatGenerator) Close() error { return nil }
