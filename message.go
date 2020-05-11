package main

import (
	"fmt"
	"strings"
	"time"
)

type Record struct {
	uid     string // UID is unique to deduplicate the stream
	fields  map[string]interface{}
}

func (tp *TablePiper) inserter() error {
	totalInserted := 0
	var orderedKeys []string

	for {
		sb := strings.Builder{}

		sb.WriteString("INSERT INTO ")
		sb.WriteString(tp.table)
		sb.WriteString("(uid")
		var values []interface{}

		tp.streamLock.Lock()
		insertLoop:
		for {
			select {
			case r := <-tp.stream:
				if totalInserted == 0 {
					orderedKeys = r.getKeyOrdering()
				}
				if len(values) == 0 {
					for _, k := range orderedKeys {
						sb.WriteString(", ")
						sb.WriteString(k)
					}
					sb.WriteString(") VALUES ")
				}
				if len(values) > 0 {
					sb.WriteString(", ")
				}
				values = append(values, r.uid)
				sb.WriteString(fmt.Sprintf("($%v", len(values)))
				for _, k := range orderedKeys {
					v := r.fields[k]
					values = append(values, v)
					sb.WriteString(fmt.Sprintf(", $%v", len(values)))
				}
				sb.WriteString(")")
				totalInserted++
				if len(values) > 250 {
					break insertLoop
				}
			default:
				if len(values) > 0 {
					break insertLoop
				} else if tp.intentToClose.Get() {
					tp.streamLock.Unlock()
					tp.wg.Done()
					return nil
				} else {
					time.Sleep(time.Millisecond * 10)
				}
			}
		}
		tp.streamLock.Unlock()
		// send to pg
		fmt.Println(sb.String())
		time.Sleep(time.Millisecond*1000)
		if tp.intentToClose.Get() {
			fmt.Println("we closin'")
			tp.wg.Done()
			return nil
		}
	}
	return nil
}

// gets some arbitrary key ordering to make things consistent
func (r *Record) getKeyOrdering() []string {
	out := make([]string, len(r.fields))
	i := 0
	for k, _ := range r.fields {
		out[i] = k
		i++
	}
	return out
}

