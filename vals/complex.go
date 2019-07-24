package vals

// Entry is a "row" of a dataset
type Entry struct {
	// Index represents this entry's numeric position in a dataset
	// this index may not necessarily refer to the overall position within the dataset
	// as things like offsets affect where the index begins
	Index int
	// Key is a string key for this entry
	// only present when the top level structure is a map
	Key string
	// Value is information contained within the row
	Value interface{}
}

// Iterator is a stream of values
type Iterator interface {
	// If the iterator is exhausted, Next returns false.
	// Otherwise it sets *p to the current element of the sequence,
	// advances the iterator, and returns true.
	Next() (e *Entry, done bool)
	Done()
}

// Keyable can return a value for a given string key
type Keyable interface {
	ValueForKey(key string) (v interface{}, err error)
}

// Indexable is a compound or complex object that can be read by index
type Indexable interface {
	ValueForIndex(i int) (v interface{}, err error)
}

// Link is a path that can be resolved
type Link string