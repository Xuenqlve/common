package schema_store

import (
	"sync"

	"github.com/xuenqlve/common/errors"
)

type SchemaKey interface {
	UniqueID() string
}

type SchemaStore interface {
	GetSchema(key SchemaKey) (any, error)
	InvalidateSchemaCache(key SchemaKey)
	InvalidateCache()
	IsInCache(key SchemaKey) bool
	Close() error
}

type LoadSchemaTool interface {
	LoadSchema(key SchemaKey) (any, error)
	Close() error
}

func NewBaseSchemaStore(load LoadSchemaTool) SchemaStore {
	return &BaseSchemaStore{
		schemas:        map[SchemaKey]any{},
		LoadSchemaTool: load,
	}
}

type BaseSchemaStore struct {
	sync.RWMutex
	schemas map[SchemaKey]any
	LoadSchemaTool
}

func (s *BaseSchemaStore) GetSchema(key SchemaKey) (any, error) {
	schema, ok := s.getFromCache(key, true)
	if ok {
		return schema, nil
	}

	s.Lock()
	defer s.Unlock()
	schema, ok = s.getFromCache(key, false)
	if ok {
		return schema, nil
	}
	schema, err := s.LoadSchema(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.schemas[key] = schema
	return schema, nil
}

func (s *BaseSchemaStore) getFromCache(key SchemaKey, lock bool) (any, bool) {
	if lock {
		s.RLock()
		defer s.RUnlock()
	}
	cachedSchema, ok := s.schemas[key]
	if ok {
		return cachedSchema, true
	}
	return nil, false
}

func (s *BaseSchemaStore) InvalidateSchemaCache(key SchemaKey) {
	s.Lock()
	defer s.Unlock()
	delete(s.schemas, key)
}

func (s *BaseSchemaStore) InvalidateCache() {
	s.Lock()
	defer s.Unlock()
	// make a new map here
	s.schemas = make(map[SchemaKey]any)
}

func (s *BaseSchemaStore) IsInCache(key SchemaKey) bool {
	s.RLock()
	defer s.RUnlock()
	if _, ok := s.schemas[key]; ok {
		return true
	} else {
		return false
	}
}

func (s *BaseSchemaStore) Close() error {
	return s.LoadSchemaTool.Close()
}
