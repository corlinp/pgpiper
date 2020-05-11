package main

import "sync"

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
