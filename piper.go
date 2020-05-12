package pgpiper

import (
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"sync"
)

type Piper struct {
	db          *sql.DB
	tablePipers map[string]*TablePiper
	intentToClose atomicBool
	wg sync.WaitGroup
}

func NewPiper(db *sql.DB) *Piper {
	return &Piper{
		db:          db,
		tablePipers: map[string]*TablePiper{},
	}
}

func (p *Piper) Close() {
	p.intentToClose.Set(true)
	p.wg.Wait()
}

func (p *Piper) Table(name string) *TablePiper {
	tp, ok := p.tablePipers[name]
	if !ok {
		tp = &TablePiper{
			Piper: p,
			table:  name,
			stream: make(chan *Record, 1024),
		}
		p.tablePipers[name] = tp
		numInserters := 10
		for i := 0; i < numInserters; i++ {
			tp.wg.Add(1)
			go tp.inserter()
		}
	}
	return tp
}

// TablePiper pipes data into a specific table
type TablePiper struct {
	*Piper
	table 		string
	stream chan *Record
	streamLock sync.Mutex
}

// AddRecord adds data to the piper. Thread-safe.
// Throws an error for unsupported types. Supported types are:
// int, float64, string, bool, time.Time
func (tp *TablePiper) AddRecord(uid string, data map[string]interface{}) error {
	if tp.intentToClose.Get() {
		return errors.New("piper is closed")
	}
	rec := Record{
		uid:    uid,
		fields: data,
	}
	tp.stream <- &rec
	return nil
}