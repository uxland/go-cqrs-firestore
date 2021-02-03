package shared

type Filter struct {
	Path  string
	Op    string
	Value interface{}
}

type ItemFactory func() interface{}

type GenericReadDB interface {
	SaveItem(transaction interface{}, id string, item interface{}) error
	LoadAllItems() ([]interface{}, error)
	LoadItem(id string) (interface{}, error)
	DeleteItem(transaction interface{}, id string) error
	UpdateItem(transaction interface{}, id string, updates interface{}) error
	ListItems(filter []Filter, limit int) ([]interface{}, error)
}
