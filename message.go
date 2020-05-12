package pgpiper

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
	var recordExample *Record

	for {
		sb := strings.Builder{}

		sb.WriteString("INSERT INTO ")
		sb.WriteString(tp.table)
		sb.WriteString("(uid")
		var values []interface{}
		// track the uids seen because of the postgres error: DO UPDATE command cannot affect row a second time
		uidsSeen := map[string]interface{}{}

		tp.streamLock.Lock()
		insertLoop:
		for {
			select {
			case r := <-tp.stream:
				if totalInserted == 0 {
					recordExample = r
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
					if _, ok := uidsSeen[r.uid]; ok {
						break insertLoop
					}
					sb.WriteString(", ")
				}
				uidsSeen[r.uid] = nil
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
		sb.WriteString(" ON CONFLICT (uid) DO UPDATE SET ")
		for i, f := range orderedKeys {
			if i != 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(f)
			sb.WriteString(" = excluded.")
			sb.WriteString(f)
		}
		sb.WriteString(";")
		// send to pg
		fmt.Println(sb.String())
		_, err := tp.db.Exec(sb.String(), values...)
		if err != nil && strings.Contains(err.Error(), "does not exist") {
			_, err = tp.db.Exec(tp.createTableSQL(recordExample))
			if err != nil {
				fmt.Println(err)
			}
			_, err = tp.db.Exec(tp.createIndexSQL())
			if err != nil {
				fmt.Println(err)
			}
			_, err = tp.db.Exec(sb.String(), values...)
			if err != nil {
				fmt.Println(err)
			}
		} else if err != nil {
			fmt.Println(err)
		}
		if tp.intentToClose.Get() {
			tp.wg.Done()
			return nil
		}
	}
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


func (tp *TablePiper) createTableSQL(r *Record) string {
	sb := strings.Builder{}
	sb.WriteString("CREATE TABLE IF NOT EXISTS ")
	sb.WriteString(tp.table)
	sb.WriteString(" (uid text")
	for k, v := range r.fields {
		sb.WriteString(", ")
		sb.WriteString(k)
		sb.WriteString(" ")
		sb.WriteString(getDatatype(v))
	}
	sb.WriteString(");")
	fmt.Println(sb.String())
	return sb.String()
}

func (tp *TablePiper) createIndexSQL() string {
	sb := strings.Builder{}
	sb.WriteString("CREATE UNIQUE INDEX IF NOT EXISTS idx_")
	sb.WriteString(tp.table)
	sb.WriteString("_uid ON ")
	sb.WriteString(tp.table)
	sb.WriteString("(uid);")
	return sb.String()
}
