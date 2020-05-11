package main

import (
	_ "github.com/lib/pq"
	"math/rand"
	"time"
)

func main() {
	//pgurl := os.Getenv("POSTGRES_URL")
	//fmt.Println(pgurl)
	//db, err := sql.Open("postgres", pgurl)
	//if err != nil {
	//	panic(err)
	//}
	//err = db.Ping()
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("connected to Postgres")

	p := NewPiper(nil)
	for i := 0; i < 1000; i++ {
		err := p.Table("best_cats").AddRecord("1", map[string]interface{}{
			"name": "jeremy",
			"weight": 27.5,
		})
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Int63n(10)))
	}

	p.Close()
}