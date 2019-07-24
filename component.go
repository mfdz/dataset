package dataset

type Component interface {
	ValueForKey(string) (interface{}, error)
	DropTransientValues()
}