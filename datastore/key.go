package datastore

import "cloud.google.com/go/datastore"

func newKey(namespace, kind, id string, parent *datastore.Key) *datastore.Key {
	key := datastore.NameKey(kind, id, parent)
	if namespace != "" {
		key.Namespace = namespace
	}
	return key
}
