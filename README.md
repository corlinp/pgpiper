# pgpiper

`pgpiper` is a go module for inserting lots of data into postgres quickly. It features automatic schema generation so you can get started with no migrations.

Sqlpiper buffers `map[string]interface{}` records into a number of concurrent insert goroutines, which do inserts as fast as possible and batch up data as it lags in the buffer.

~50X faster than inserting records one at a time sequentially

Usage:
```go
db, _ := sql.Open("postgres", "postgres://myurl/")

p := NewPiper(db)
p.Table("fat_cats").AddRecord("1", map[string]interface{}{
    "name": "jeremy",
    "weight": 27.5,
})
p.Close()
```

In this example, a table called `fat_cats` will be created and the record piped in with a uid of 1.

UIDs are unique. Adding to the same UID will overwrite the old data for that UID.

*pgpiper barely works and is still under development*
