package shared

import "reflect"

type Filter struct {
	Path  string
	Op    string
	Value interface{}
}
type GenericReadDB interface {
	SaveItem(transaction interface{}, id string, item interface{}) error
	LoadAllItems() ([]reflect.Type, error)
	LoadItem(id string) (interface{}, error)
	DeleteItem(transaction interface{}, id string) error
	UpdateItem(transaction interface{}, id string, updates interface{}) error
	ListItems(filter []Filter, limit int) ([]reflect.Type, error)
}
