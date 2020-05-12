package pgpiper

import (
	"sync"
	"time"
)

type atomicBool struct {
	sync.RWMutex
	value bool
}

func (a *atomicBool) Get() bool {
	a.RLock()
	a.RUnlock()
	return a.value
}

func (a *atomicBool) Set(v bool) {
	a.Lock()
	a.value = v
	a.Unlock()
}

func getDatatype(v interface{}) string {
	switch v.(type) {
	case int, int32:
		return "integer"
	case int64:
		return "bigint"
	case float64:
		return "float8"
	case float32:
		return "float4"
	case bool:
		return "boolean"
	case string:
		return "text"
	case time.Time:
		return "timestamptz"
	default:
		return ""
	}
}